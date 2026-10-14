//! Write task: prioritized control over commands; encode + batched flush on `FramedWrite`.
//!
//! Inbound PUBACKs and outbound PUBLISH frames are fed via [`SinkExt::feed`] (or encoded into the
//! write buffer) and flushed once after draining ready siblings — fewer TCP syscalls under load.
//! PING / DISCONNECT still flush immediately.

mod acks;
mod commands;
mod io;
mod keepalive;

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::SinkExt;
use tokio::io::AsyncWrite;
use tokio::sync::oneshot;
use tokio_util::codec::FramedWrite;
use tokio_util::sync::CancellationToken;

use bytestring::ByteString;

use crate::codec::{Disconnect, Packet, PublishMessage, QoS, SubscriptionOptions, V5Codec};
use crate::connection::packet_id::{InflightPots, PacketIdMap};
use crate::connection::subscriptions::SubscriptionRegistry;
use crate::connection::{
    CommandReceiver, ControlMessage, ControlReceiver, Generation, PublishReply, StopSignal, SubscribeReply,
    UnsubscribeReply,
};
use crate::error::{ProtocolError, TransportError};

use self::acks::{complete_subscribe_ack, complete_unsubscribe_ack, fail_waiters_for_outcome};
use self::commands::{
    CommandDispatch, CommandDispatchCtx, dispatch_client_command, resend_carried, send_parked_publishes,
};
use self::io::{
    ControlCoalesceCtx, FlushAbort, GracefulDisconnectCtx, drain_ready_control_coalesce, feed_inbound_publish_ack,
    flush_writes, graceful_disconnect, handle_deferred_control, send_packet, send_pending_protocol_disconnect,
};
use self::keepalive::keep_alive_ping_timer;

#[allow(unused_imports)] // re-exported for crate callers / write::tests via super::*
pub(crate) use self::acks::{ack_generation_applies, complete_publish_ack};

// Bring submodule helpers into this module's namespace so `write::tests` can `use super::*`.
#[cfg(test)]
#[allow(unused_imports)]
use self::acks::{app_error_from_write_outcome, fail_all_waiters};
#[cfg(test)]
#[allow(unused_imports)]
use self::commands::{
    finish_publish_encode_error, handle_publish, validate_subscribe_request, validate_unsubscribe_request,
};
#[cfg(test)]
#[allow(unused_imports)]
use crate::codec::PublishAck;
#[cfg(test)]
#[allow(unused_imports)]
use crate::error::AppError;
#[cfg(test)]
#[allow(unused_imports)]
use tokio::sync::mpsc;

/// Soft threshold: flush the write buffer once it reaches this many encoded bytes
/// mid-coalesce (still followed by an explicit flush at the end of each batch).
///
/// Larger values reduce syscall count on pipelined/large-payload bursts; the
/// adaptive coalesce path still flushes every batch, so sparse serial publishes
/// are not held waiting for this threshold.
pub(super) const WRITE_BUFFER_SOFT_FLUSH_BYTES: usize = 16 * 1024;

pub(super) use crate::connection::io_batch::MAX_READY_DRAIN;

/// Upper bound on QoS-1 PUBLISHes held while the broker's Receive Maximum window is full.
pub(super) const MAX_PARKED_PUBLISHES: usize = crate::options::COMMAND_CHANNEL_CAPACITY;

/// Outcome of the write task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteOutcome {
    /// Cancelled by the coordinator.
    Cancelled,
    /// Graceful shutdown command received (DISCONNECT sent when possible).
    Shutdown,
    /// Test/skeleton forced exit (supervision hook).
    #[cfg(test)]
    StubExit,
    /// Command channel closed.
    CommandChannelClosed,
    /// Control channel closed.
    ControlChannelClosed,
    /// Socket / encode I/O failure.
    Transport(TransportError),
    /// Encode / protocol failure while writing.
    Protocol(ProtocolError),
}

/// Outstanding subscribe/unsubscribe waiters keyed by Packet Identifier.
pub(super) struct SubUnsubWaiters {
    pub(super) subscribe: PacketIdMap<PendingSubscribe>,
    pub(super) unsubscribe: PacketIdMap<PendingUnsubscribe>,
}

pub(super) struct PendingSubscribe {
    pub(super) reply: SubscribeReply,
    pub(super) filters: Vec<(ByteString, SubscriptionOptions)>,
    /// Subscription Identifier the SUBSCRIBE carried (kept in the registry for resubscribe).
    pub(super) id: Option<std::num::NonZeroU32>,
}

pub(super) struct PendingUnsubscribe {
    pub(super) reply: UnsubscribeReply,
    pub(super) filters: Vec<ByteString>,
}

impl SubUnsubWaiters {
    fn new() -> Self {
        Self {
            subscribe: PacketIdMap::default(),
            unsubscribe: PacketIdMap::default(),
        }
    }
}

/// Outstanding outbound QoS-1 publish waiters keyed by Packet Identifier.
///
/// Waiters are completed only by a [`ControlMessage::PublishAck`] whose `generation`
/// matches the write task's live [`Generation`].
///
/// After a PUBLISH is encoded, the Packet Identifier stays reserved until that PUBACK
/// (or session teardown) even if the caller drops the oneshot ([MQTT-2.2.1-3]).
///
/// `parked` is a FIFO of QoS-1 PUBLISHes that arrived while the broker's Receive Maximum
/// window was full ([MQTT-4.9.0-2]). They are sent in order as PUBACKs free slots.
/// SUBSCRIBE / UNSUBSCRIBE keep flowing past them; only when the queue reaches
/// [`MAX_PARKED_PUBLISHES`] does the write task stop reading commands, so backpressure
/// reaches the bounded command channel instead of growing memory.
///
/// With a resumable session each in-flight entry also keeps its message, so that a
/// connection loss can hand it to the next connection for resending. Carried ones that do not
/// fit the new connection's Receive Maximum wait in `resend_parked`, ahead of `parked`.
#[derive(Debug, Default)]
pub(crate) struct PublishWaiters {
    pub(super) by_id: PacketIdMap<InflightPublish>,
    pub(super) parked: VecDeque<PendingOutboundPublish>,
    /// Unacknowledged publishes of a resumed session still to be resent (DUP, original id).
    pub(super) resend_parked: VecDeque<CarriedPublish>,
    pub(super) carry: Option<SessionCarry>,
    /// Keep each in-flight message for a resend (resumable session).
    keep_messages: bool,
    /// Send order of in-flight publishes, for ordered resend after reconnect.
    next_seq: u64,
    /// Outbound Topic Alias bindings of this connection.
    pub(super) aliases: crate::connection::topic_alias::OutboundTopicAliases,
    /// Counters behind [`crate::MqttClient::stats`] (travels with the publish paths).
    pub(super) stats: crate::stats::Stats,
}

/// One QoS-1 PUBLISH on the wire awaiting its PUBACK.
#[derive(Debug)]
pub(crate) struct InflightPublish {
    pub(super) reply: PublishReply,
    /// Message + retain flag, kept only for resumable sessions.
    pub(super) resend: Option<(PublishMessage, bool)>,
    /// Position in send order (see [`PublishWaiters`]).
    pub(super) seq: u64,
}

/// Unacknowledged QoS-1 PUBLISH carried from one connection to the next of a resumed
/// session ([MQTT-4.4.0-1]); the caller's completion stays attached.
#[derive(Debug)]
pub(crate) struct CarriedPublish {
    pub(crate) packet_id: NonZeroU16,
    pub(crate) message: PublishMessage,
    pub(crate) retain: bool,
    pub(crate) reply: PublishReply,
    /// Original send order ([MQTT-4.6.0-1]).
    pub(crate) seq: u64,
}

/// Hand-over slot between connections, owned by the [`Connection`](crate::Connection).
///
/// On connection loss the write task moves what it still owes the application into it; the
/// next connection's write task sends that first, before any new command.
pub(crate) type SessionCarry = Arc<std::sync::Mutex<Carry>>;

/// Contents of a [`SessionCarry`].
#[derive(Debug, Default)]
pub(crate) struct Carry {
    /// Session outlives the connection (Clean Start 0, Session Expiry > 0): unacknowledged
    /// QoS-1 publishes are session state and get resent ([MQTT-4.4.0-1]).
    pub(crate) resumable: bool,
    /// Unacknowledged QoS-1 publishes (resumable sessions only), resent with DUP.
    pub(crate) unacked: Vec<CarriedPublish>,
    /// Publishes parked behind the broker's Receive Maximum, never put on the wire; sent like
    /// new ones, before the commands still waiting in the command channel.
    pub(crate) unsent: VecDeque<PendingOutboundPublish>,
}

impl Carry {
    pub(crate) fn new(resumable: bool) -> Self {
        Self {
            resumable,
            ..Self::default()
        }
    }

    /// Takes everything out, failing nothing (caller decides).
    pub(crate) fn take_all(&mut self) -> (Vec<CarriedPublish>, VecDeque<PendingOutboundPublish>) {
        (std::mem::take(&mut self.unacked), std::mem::take(&mut self.unsent))
    }
}

impl PublishWaiters {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn with_carry(carry: Option<SessionCarry>) -> Self {
        let keep_messages = carry.as_ref().is_some_and(|carry| {
            carry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .resumable
        });
        Self {
            carry,
            keep_messages,
            ..Self::default()
        }
    }

    /// Registers an encoded QoS-1 publish; keeps the message when the session is resumable.
    pub(super) fn insert_sent(&mut self, packet_id: NonZeroU16, publish: PendingOutboundPublish) {
        let resend = self.keep_messages.then_some((publish.message, publish.retain));
        let seq = self.next_seq;
        self.next_seq += 1;
        self.by_id.insert(
            packet_id,
            InflightPublish {
                reply: publish.reply,
                resend,
                seq,
            },
        );
    }

    /// Whether the parked queue is full (the write task must stop reading commands).
    #[inline]
    pub(super) fn parked_full(&self) -> bool {
        self.parked.len() >= MAX_PARKED_PUBLISHES
    }

    /// Publishes waiting for a slot in the broker's Receive Maximum window (resends first).
    #[inline]
    pub(super) fn waiting_for_window(&self) -> usize {
        self.resend_parked.len() + self.parked.len()
    }

    #[cfg(test)]
    pub(super) fn insert(&mut self, packet_id: NonZeroU16, reply: PublishReply) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.by_id.insert(
            packet_id,
            InflightPublish {
                reply,
                resend: None,
                seq,
            },
        );
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Outbound PUBLISH command from the client API.
#[derive(Debug)]
pub(crate) struct PendingOutboundPublish {
    pub(super) message: PublishMessage,
    pub(super) qos: QoS,
    pub(super) retain: bool,
    pub(super) reply: PublishReply,
}

/// CONNACK-derived outbound PUBLISH limits ([MQTT-3.2.2-11], [MQTT-3.2.2-14]).
#[derive(Debug, Clone, Copy)]
pub(super) struct BrokerPublishLimits {
    pub(super) max_qos: QoS,
    pub(super) retain_available: bool,
}

/// Negotiated session parameters used by the write task (generation binding, keep-alive).
#[derive(Debug, Clone)]
pub(crate) struct WriteSession {
    /// Connection generation for stale PUBACK discard.
    pub generation: Generation,
    /// Effective keep-alive after CONNACK override.
    pub keep_alive: Duration,
    /// Shared subscription registry for app subscribe/unsubscribe updates.
    pub subscription_registry: Arc<RwLock<SubscriptionRegistry>>,
    /// Session cancel (peer close / supervisor).
    pub cancel: StopSignal,
    /// App graceful-shutdown notify — selected before [`Self::cancel`].
    pub shutdown_notify: CancellationToken,
    /// Budget for pending inbound PUBACK + DISCONNECT flush ([`crate::DEFAULT_GRACEFUL_DISCONNECT_TIMEOUT`]).
    pub graceful_disconnect_timeout: Duration,
    /// Broker Maximum QoS from CONNACK ([MQTT-3.2.2-11]).
    pub max_qos: QoS,
    /// Broker Retain Available from CONNACK ([MQTT-3.2.2-14]).
    pub retain_available: bool,
    /// Client Receive Maximum from CONNECT (inbound QoS1 window / adaptive coalesce).
    pub receive_maximum: NonZeroU16,
    /// Read-task flag: true while blocked on a full handoff (keepalive watchdog exception).
    pub handoff_stalled: Arc<AtomicBool>,
    /// Read-task flag: set on any decoded inbound frame (keepalive liveness).
    pub inbound_activity: Arc<AtomicBool>,
    /// Optional bench hook — incremented per inbound QoS-1 PUBACK fed to the write buffer.
    pub bench_inbound_publish_ack_counter: Option<Arc<AtomicUsize>>,
    /// Hand-over to the next connection ([`SessionCarry`]); `None` only in unit tests.
    pub session_carry: Option<SessionCarry>,
    /// Stops the paired read task; cancelled by graceful shutdown after the outbound drain.
    pub read_stop: StopSignal,
    /// Outbound Topic Aliases to use on this connection (already capped by the broker).
    pub outbound_topic_aliases: u16,
    /// CONNACK Subscription Identifiers Available.
    pub subscription_identifiers_available: bool,
    /// Counters behind [`crate::MqttClient::stats`].
    pub stats: crate::stats::Stats,
}

/// Runs the write task until cancelled, shut down, or signalled to exit.
///
/// Control messages are prioritized over application commands.
/// Biased `select!` order: `shutdown_notify` and session `cancel` before the keep-alive
/// tick so graceful DISCONNECT / cancel are not displaced by a PINGRESP watchdog kill.
/// Encode + flush use the write half's dedicated [`crate::codec::V5Codec`].
///
/// `read_done` completes when the paired read task exits; graceful shutdown drains inbound
/// PUBACKs until then, then sends DISCONNECT.
///
/// `command_rx` is the client's command channel, which outlives the session: commands still in
/// it when the connection is lost go out on the next one.
pub(crate) async fn run<W>(
    mut control_rx: ControlReceiver,
    command_rx: &mut CommandReceiver,
    #[cfg(test)] mut stub_exit_rx: mpsc::Receiver<()>,
    mut framed: FramedWrite<W, V5Codec>,
    session: WriteSession,
    mut inflight: InflightPots,
    read_done: oneshot::Receiver<()>,
) -> WriteOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let subscription_registry = session.subscription_registry;
    let cancel = session.cancel;
    let shutdown_notify = session.shutdown_notify;
    let graceful_disconnect_timeout = session.graceful_disconnect_timeout;
    let mut waiters = SubUnsubWaiters::new();
    let mut publish_waiters = PublishWaiters::with_carry(session.session_carry);
    publish_waiters.aliases = crate::connection::topic_alias::OutboundTopicAliases::new(session.outbound_topic_aliases);
    publish_waiters.stats = session.stats;
    let mut ping_timer = keep_alive_ping_timer(session.keep_alive);
    let keep_alive = session.keep_alive;
    let handoff_stalled = session.handoff_stalled;
    let inbound_activity = session.inbound_activity;
    let inbound_publish_ack_counter = session.bench_inbound_publish_ack_counter;
    let read_stop = session.read_stop;
    let subscription_identifiers_available = session.subscription_identifiers_available;
    let mut awaiting_pingresp = false;
    let generation = session.generation;
    let receive_maximum = session.receive_maximum;
    let publish_limits = BrokerPublishLimits {
        max_qos: session.max_qos,
        retain_available: session.retain_available,
    };
    // Hot-path flushes race both tokens so stuck poll_write cannot block cancel/shutdown, and
    // give up after 1.5 x Keep Alive, the span after which a broker gives up on a silent
    // client (§3.1.2.10).
    let flush_abort = FlushAbort::Hot {
        cancel: &cancel,
        shutdown_notify: &shutdown_notify,
        stall_limit: (!keep_alive.is_zero()).then(|| keep_alive * 3 / 2),
    };

    // What the previous connection still owed: unacknowledged QoS-1 PUBLISHes first (resumed
    // session; original ids, DUP set, [MQTT-4.4.0-1]), then publishes it never sent.
    if let Err(outcome) = resend_carried(
        &mut framed,
        &mut inflight,
        &mut publish_waiters,
        publish_limits,
        flush_abort,
    )
    .await
    {
        return finish_hot_path_write_outcome(
            &mut framed,
            GracefulDisconnectCtx {
                control_rx: &mut control_rx,
                waiters: &mut waiters,
                publish_waiters: &mut publish_waiters,
                inflight: &mut inflight,
                flush_budget: graceful_disconnect_timeout,
                command_rx,
                publish_limits,
                subscription_registry: &subscription_registry,
                generation,
                read_stop: &read_stop,
            },
            read_done,
            outcome,
        )
        .await;
    }

    // Created once, not per iteration: each fresh `cancelled()` future registers with (and on
    // drop deregisters from) the token under its mutex.
    let shutdown_wait = shutdown_notify.cancelled();
    let cancel_wait = cancel.cancelled();
    tokio::pin!(shutdown_wait, cancel_wait);

    loop {
        // A PUBACK handled in the previous iteration may have freed window slots.
        if publish_waiters.waiting_for_window() > 0 && inflight.has_publish_capacity() {
            let sent = match send_parked_publishes(&mut framed, &mut inflight, &mut publish_waiters) {
                Ok(()) => flush_writes(&mut framed, flush_abort).await,
                Err(outcome) => Err(outcome),
            };
            if let Err(outcome) = sent {
                return finish_hot_path_write_outcome(
                    &mut framed,
                    GracefulDisconnectCtx {
                        control_rx: &mut control_rx,
                        waiters: &mut waiters,
                        publish_waiters: &mut publish_waiters,
                        inflight: &mut inflight,
                        flush_budget: graceful_disconnect_timeout,
                        command_rx,
                        publish_limits,
                        subscription_registry: &subscription_registry,
                        generation,
                        read_stop: &read_stop,
                    },
                    read_done,
                    outcome,
                )
                .await;
            }
        }

        publish_waiters
            .stats
            .outbound_window(publish_waiters.by_id.len(), publish_waiters.waiting_for_window());

        // Do not put `#[cfg(test)]` on a `tokio::select!` arm — the macro rejects it.
        #[cfg(test)]
        let stub_exit = stub_exit_rx.recv();
        #[cfg(not(test))]
        let stub_exit = std::future::pending::<Option<()>>();

        tokio::select! {
            biased;
            // App shutdown / session cancel before ping so the watchdog cannot turn a graceful
            // teardown into `PINGRESP timed out` while read has already stopped marking activity.
            () = &mut shutdown_wait => {
                return graceful_disconnect(
                    &mut framed,
                    GracefulDisconnectCtx {
                        control_rx: &mut control_rx,
                        waiters: &mut waiters,
                        publish_waiters: &mut publish_waiters,
                        inflight: &mut inflight,
                        flush_budget: graceful_disconnect_timeout,
                        command_rx,
                        publish_limits,
                        subscription_registry: &subscription_registry,
                        generation,
                        read_stop: &read_stop,
                    },
                    read_done,
                )
                .await;
            }
            () = &mut cancel_wait => {
                // Peer close / supervisor cancel: outstanding QoS1 waiters must not hang
                // on connection loss.
                send_pending_protocol_disconnect(&mut framed, &mut control_rx).await;
                return finish_write_outcome(
                    &mut waiters,
                    &mut publish_waiters,
                    &mut inflight,
                    WriteOutcome::Cancelled,
                );
            }
            ping_tick = async {
                match ping_timer.as_mut() {
                    Some(timer) => timer.tick().await,
                    None => std::future::pending().await,
                }
            } => {
                let _ = ping_tick;
                // Belt-and-suspenders if notify fired in the same wake as this tick.
                if shutdown_notify.is_cancelled() || cancel.is_cancelled() {
                    continue;
                }
                // Any inbound since the last tick proves broker liveness (not only PINGRESP).
                // AcqRel pairs with read-task Release stores — do not weaken to Relaxed
                // (delayed visibility can false-trigger PINGRESP timeout during handoff stall).
                if inbound_activity.swap(false, Ordering::AcqRel) {
                    awaiting_pingresp = false;
                }
                // MQTT §3.1.2.10 SHOULD: no broker traffic within ~keep_alive after PINGREQ → close.
                // Exception: full-handoff stall (self-induced unread) must not look like a dead peer.
                // Acquire pairs with read-task Release on handoff_stalled.
                if awaiting_pingresp && !handoff_stalled.load(Ordering::Acquire) {
                    return finish_write_outcome(
                        &mut waiters,
                        &mut publish_waiters,
                        &mut inflight,
                        WriteOutcome::Transport(TransportError::Connection(format!(
                            "PINGRESP timed out after {keep_alive:?}"
                        ))),
                    );
                }
                if let Err(outcome) = send_packet(&mut framed, Packet::PingRequest, flush_abort).await {
                    return finish_hot_path_write_outcome(
                        &mut framed,
                        GracefulDisconnectCtx {
                            control_rx: &mut control_rx,
                            waiters: &mut waiters,
                            publish_waiters: &mut publish_waiters,
                            inflight: &mut inflight,
                            flush_budget: graceful_disconnect_timeout,
                            command_rx,
                            publish_limits,
                            subscription_registry: &subscription_registry,
                            generation,
                            read_stop: &read_stop,
                        },
                        read_done,
                        outcome,
                    )
                    .await;
                }
                publish_waiters.stats.ping_sent();
                awaiting_pingresp = true;
            }
            control = control_rx.recv() => {
                match control {
                    #[cfg(test)]
                    Some(ControlMessage::StubExitWrite) => return WriteOutcome::StubExit,
                    #[cfg(test)]
                    Some(ControlMessage::PingReq) => {
                        if shutdown_notify.is_cancelled() || cancel.is_cancelled() {
                            continue;
                        }
                        if inbound_activity.swap(false, Ordering::AcqRel) {
                            awaiting_pingresp = false;
                        }
                        if awaiting_pingresp && !handoff_stalled.load(Ordering::Acquire) {
                            return finish_write_outcome(
                                &mut waiters,
                                &mut publish_waiters,
                                &mut inflight,
                                WriteOutcome::Transport(TransportError::Connection(format!(
                                    "PINGRESP timed out after {keep_alive:?}"
                                ))),
                            );
                        }
                        if let Err(outcome) =
                            send_packet(&mut framed, Packet::PingRequest, flush_abort).await
                        {
                            return finish_hot_path_write_outcome(
                                &mut framed,
                                GracefulDisconnectCtx {
                                    control_rx: &mut control_rx,
                                    waiters: &mut waiters,
                                    publish_waiters: &mut publish_waiters,
                                    inflight: &mut inflight,
                                    flush_budget: graceful_disconnect_timeout,
                                    command_rx,
                                    publish_limits,
                                    subscription_registry: &subscription_registry,
                                    generation,
                                    read_stop: &read_stop,
                                },
                                read_done,
                                outcome,
                            )
                            .await;
                        }
                        awaiting_pingresp = true;
                    }
                    #[cfg(test)]
                    Some(ControlMessage::Disconnect) => {
                        // DISCONNECT is terminal ([MQTT-3.14.4-1/2]) — do not process further control.
                        if let Err(outcome) = send_packet(
                            &mut framed,
                            Packet::Disconnect(Disconnect::default()),
                            flush_abort,
                        )
                        .await
                        {
                            return finish_hot_path_write_outcome(
                                &mut framed,
                                GracefulDisconnectCtx {
                                    control_rx: &mut control_rx,
                                    waiters: &mut waiters,
                                    publish_waiters: &mut publish_waiters,
                                    inflight: &mut inflight,
                                    flush_budget: graceful_disconnect_timeout,
                                    command_rx,
                                    publish_limits,
                                    subscription_registry: &subscription_registry,
                                    generation,
                                    read_stop: &read_stop,
                                },
                                read_done,
                                outcome,
                            )
                            .await;
                        }
                        return finish_write_outcome(
                            &mut waiters,
                            &mut publish_waiters,
                            &mut inflight,
                            WriteOutcome::Shutdown,
                        );
                    }
                    Some(ControlMessage::ProtocolDisconnect(reason)) => {
                        // DISCONNECT is terminal ([MQTT-3.14.4-1/2]) — do not process further control.
                        if let Err(outcome) = send_packet(
                            &mut framed,
                            Packet::Disconnect(Disconnect::new(reason)),
                            flush_abort,
                        )
                        .await
                        {
                            return finish_hot_path_write_outcome(
                                &mut framed,
                                GracefulDisconnectCtx {
                                    control_rx: &mut control_rx,
                                    waiters: &mut waiters,
                                    publish_waiters: &mut publish_waiters,
                                    inflight: &mut inflight,
                                    flush_budget: graceful_disconnect_timeout,
                                    command_rx,
                                    publish_limits,
                                    subscription_registry: &subscription_registry,
                                    generation,
                                    read_stop: &read_stop,
                                },
                                read_done,
                                outcome,
                            )
                            .await;
                        }
                        fail_waiters_for_outcome(
                            &mut waiters,
                            &mut publish_waiters,
                            &mut inflight,
                            &WriteOutcome::Cancelled,
                        );
                        // Close the Network Connection immediately ([MQTT-3.14.4-2]) before waiting
                        // for supervisor cancel (so typed read ProtocolError still wins).
                        let _ = framed.close().await;
                        drop(framed);
                        cancel.cancelled().await;
                        return WriteOutcome::Cancelled;
                    }
                    Some(ControlMessage::SubscribeAck {
                        ack,
                        generation: ack_generation,
                    }) => {
                        complete_subscribe_ack(
                            &mut waiters,
                            &mut inflight,
                            &subscription_registry,
                            generation,
                            ack_generation,
                            ack,
                        );
                    }
                    Some(ControlMessage::UnsubscribeAck {
                        ack,
                        generation: ack_generation,
                    }) => {
                        complete_unsubscribe_ack(
                            &mut waiters,
                            &mut inflight,
                            &subscription_registry,
                            generation,
                            ack_generation,
                            ack,
                        );
                    }
                    Some(ControlMessage::PublishAck { ack, generation: ack_generation }) => {
                        complete_publish_ack(
                            &mut publish_waiters,
                            &mut inflight,
                            generation,
                            ack_generation,
                            ack,
                        );
                    }
                    Some(ControlMessage::AckInboundPublish { packet_id }) => {
                        if let Err(outcome) =
                            feed_inbound_publish_ack(&mut framed, packet_id, inbound_publish_ack_counter.as_ref())
                        {
                            return finish_write_outcome(
                                &mut waiters,
                                &mut publish_waiters,
                                &mut inflight,
                                outcome,
                            );
                        }
                        match drain_ready_control_coalesce(
                            &mut ControlCoalesceCtx {
                                framed: &mut framed,
                                control_rx: &mut control_rx,
                                command_rx,
                                waiters: &mut waiters,
                                publish_waiters: &mut publish_waiters,
                                inflight: &mut inflight,
                                subscription_registry: &subscription_registry,
                                generation,
                                receive_maximum,
                                inbound_publish_ack_counter: inbound_publish_ack_counter.as_ref(),
                                flush_abort,
                            },
                            1, // already fed the triggering PUBACK
                        )
                        .await
                        {
                            Ok(None) => {
                                if let Err(outcome) = flush_writes(&mut framed, flush_abort).await {
                                    return finish_hot_path_write_outcome(
                                        &mut framed,
                                        GracefulDisconnectCtx {
                                            control_rx: &mut control_rx,
                                            waiters: &mut waiters,
                                            publish_waiters: &mut publish_waiters,
                                            inflight: &mut inflight,
                                            flush_budget: graceful_disconnect_timeout,
                                            command_rx,
                                            publish_limits,
                                            subscription_registry: &subscription_registry,
                                            generation,
                                            read_stop: &read_stop,
                                        },
                                        read_done,
                                        outcome,
                                    )
                                    .await;
                                }
                            }
                            Ok(Some(deferred)) => {
                                if let Err(outcome) = flush_writes(&mut framed, flush_abort).await {
                                    return finish_hot_path_write_outcome(
                                        &mut framed,
                                        GracefulDisconnectCtx {
                                            control_rx: &mut control_rx,
                                            waiters: &mut waiters,
                                            publish_waiters: &mut publish_waiters,
                                            inflight: &mut inflight,
                                            flush_budget: graceful_disconnect_timeout,
                                            command_rx,
                                            publish_limits,
                                            subscription_registry: &subscription_registry,
                                            generation,
                                            read_stop: &read_stop,
                                        },
                                        read_done,
                                        outcome,
                                    )
                                    .await;
                                }
                                // Deferred non-coalesceable control (PING / DISCONNECT / …).
                                match handle_deferred_control(
                                    deferred,
                                    &mut framed,
                                    flush_abort,
                                    inbound_publish_ack_counter.as_ref(),
                                )
                                .await
                                {
                                    None => {}
                                    Some(outcome) => {
                                        return finish_hot_path_write_outcome(
                                            &mut framed,
                                            GracefulDisconnectCtx {
                                                control_rx: &mut control_rx,
                                                waiters: &mut waiters,
                                                publish_waiters: &mut publish_waiters,
                                                inflight: &mut inflight,
                                                flush_budget: graceful_disconnect_timeout,
                                                command_rx,
                                                publish_limits,
                                                subscription_registry: &subscription_registry,
                                                generation,
                                                read_stop: &read_stop,
                                            },
                                            read_done,
                                            outcome,
                                        )
                                        .await;
                                    }
                                }
                            }
                            Err(outcome) => {
                                return finish_hot_path_write_outcome(
                                    &mut framed,
                                    GracefulDisconnectCtx {
                                        control_rx: &mut control_rx,
                                        waiters: &mut waiters,
                                        publish_waiters: &mut publish_waiters,
                                        inflight: &mut inflight,
                                        flush_budget: graceful_disconnect_timeout,
                                        command_rx,
                                        publish_limits,
                                        subscription_registry: &subscription_registry,
                                        generation,
                                        read_stop: &read_stop,
                                    },
                                    read_done,
                                    outcome,
                                )
                                .await;
                            }
                        }
                    }
                    #[cfg(test)]
                    Some(ControlMessage::StubExitRead) => {}
                    None => {
                        return finish_write_outcome(
                            &mut waiters,
                            &mut publish_waiters,
                            &mut inflight,
                            WriteOutcome::ControlChannelClosed,
                        );
                    }
                }
            }
            exit = stub_exit => {
                #[cfg(test)]
                {
                    let _ = exit;
                    return finish_write_outcome(
                        &mut waiters,
                        &mut publish_waiters,
                        &mut inflight,
                        WriteOutcome::StubExit,
                    );
                }
                #[cfg(not(test))]
                {
                    let _ = exit;
                    unreachable!("stub_exit is pending in release builds");
                }
            }
            // Full parked queue: leave commands in the bounded channel (backpressure).
            command = command_rx.recv(), if !publish_waiters.parked_full() => {
                let Some(cmd) = command else {
                    return finish_write_outcome(
                        &mut waiters,
                        &mut publish_waiters,
                        &mut inflight,
                        WriteOutcome::CommandChannelClosed,
                    );
                };
                match dispatch_client_command(
                    cmd,
                    &mut CommandDispatchCtx {
                        framed: &mut framed,
                        command_rx,
                        waiters: &mut waiters,
                        publish_waiters: &mut publish_waiters,
                        inflight: &mut inflight,
                        publish_limits,
                        receive_maximum,
                        flush_abort,
                        subscription_identifiers_available,
                    },
                    true, // primary recv path coalesces ready PUBLISHes
                )
                .await
                {
                    Ok(CommandDispatch::Continue) => {}
                    #[cfg(test)]
                    Ok(CommandDispatch::GracefulShutdown) => {
                        return graceful_disconnect(
                            &mut framed,
                            GracefulDisconnectCtx {
                                control_rx: &mut control_rx,
                                waiters: &mut waiters,
                                publish_waiters: &mut publish_waiters,
                                inflight: &mut inflight,
                                flush_budget: graceful_disconnect_timeout,
                                command_rx,
                                publish_limits,
                                subscription_registry: &subscription_registry,
                                generation,
                                read_stop: &read_stop,
                            },
                            read_done,
                        )
                        .await;
                    }
                    Err(outcome) => {
                        return finish_hot_path_write_outcome(
                            &mut framed,
                            GracefulDisconnectCtx {
                                control_rx: &mut control_rx,
                                waiters: &mut waiters,
                                publish_waiters: &mut publish_waiters,
                                inflight: &mut inflight,
                                flush_budget: graceful_disconnect_timeout,
                                command_rx,
                                publish_limits,
                                subscription_registry: &subscription_registry,
                                generation,
                                read_stop: &read_stop,
                            },
                            read_done,
                            outcome,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

/// Fail waiters for `outcome` and return it (exit helper for the write loop).
fn finish_write_outcome(
    waiters: &mut SubUnsubWaiters,
    publish_waiters: &mut PublishWaiters,
    inflight: &mut InflightPots,
    outcome: WriteOutcome,
) -> WriteOutcome {
    fail_waiters_for_outcome(waiters, publish_waiters, inflight, &outcome);
    outcome
}

/// Hot-path flush/send abort: `Shutdown` means `shutdown_notify` won — enter graceful
/// DISCONNECT (same as the top-level notify arm). `Cancelled` and other outcomes finish
/// waiters and exit without DISCONNECT.
async fn finish_hot_path_write_outcome<W>(
    framed: &mut FramedWrite<W, V5Codec>,
    graceful: GracefulDisconnectCtx<'_>,
    read_done: oneshot::Receiver<()>,
    outcome: WriteOutcome,
) -> WriteOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    match outcome {
        WriteOutcome::Shutdown => graceful_disconnect(framed, graceful, read_done).await,
        other => finish_write_outcome(graceful.waiters, graceful.publish_waiters, graceful.inflight, other),
    }
}

/// Test-only synchronization gate for verifying that pending inbound PUBACKs flush before DISCONNECT.
#[cfg(test)]
pub(crate) mod shutdown_test_hook {
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;

    /// Pause points around the write-task Shutdown arm.
    #[derive(Debug, Default)]
    pub struct Gate {
        /// Signalled when the write task enters `ClientCommand::Shutdown`.
        pub entered: Notify,
        /// Test notifies this after queuing a late `AckInboundPublish`.
        pub release: Notify,
    }

    static INSTALLED: Mutex<Option<Arc<Gate>>> = Mutex::new(None);

    /// Installs a gate for the next Shutdown handled by the write task.
    pub fn install(gate: Arc<Gate>) {
        *INSTALLED.lock().expect("shutdown test hook lock") = Some(gate);
    }

    /// Takes the installed gate (if any) when Shutdown is entered.
    pub(super) fn take() -> Option<Arc<Gate>> {
        INSTALLED.lock().expect("shutdown test hook lock").take()
    }
}
