//! Per-connection channel types and capacities.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytestring::ByteString;
use tokio::sync::{mpsc, oneshot};

use crate::codec::{
    DisconnectReasonCode, PublishAck, PublishMessage, QoS, SubscribeAck, SubscriptionOptions, UnsubscribeAck,
};
use crate::error::AppError;

use super::Generation;

/// Floor for the prioritized control path to the write task.
///
/// Used as [`control_channel_capacity`] when `receive_maximum` is smaller. Keeps a
/// modest queue for read-burst PUBACK coalescing under default session windows.
pub const CONTROL_CHANNEL_CAPACITY: usize = 128;

/// Capacity of the read→write control channel for a session.
///
/// `max([`CONTROL_CHANNEL_CAPACITY`], receive_maximum)` so inbound `AckInboundPublish`
/// (and outbound `PublishAck`) can queue up to one full MQTT receive window without
/// parking the read task, while default sessions stay at the 128 floor.
#[inline]
pub fn control_channel_capacity(receive_maximum: NonZeroU16) -> usize {
    CONTROL_CHANNEL_CAPACITY.max(usize::from(receive_maximum.get()))
}

/// Prioritized control messages to the write task (PUBACK, protocol DISCONNECT, SUBACK).
///
/// This path is separate from [`ClientCommand`] so control traffic is never
/// displaced by application publish/subscribe work.
///
/// Keep-alive PINGREQ is scheduled by the write-task timer as [`crate::codec::Packet::PingRequest`],
/// not via this enum. Test-only variants inject control traffic for isolation tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlMessage {
    /// Test hook: inject a keep-alive probe on the control path (prod uses the write-task timer).
    #[cfg(test)]
    #[doc(hidden)]
    PingReq,
    /// Test hook: graceful disconnect (Reason Code 0x00) via control (prod uses Shutdown / notify).
    #[cfg(test)]
    #[doc(hidden)]
    #[expect(dead_code, reason = "test-only variant; production uses Shutdown / notify")]
    Disconnect,
    /// DISCONNECT with this reason (Malformed Packet, Protocol Error, …) queued by the read
    /// task before it stops on a bad inbound packet.
    ProtocolDisconnect(DisconnectReasonCode),
    /// Broker SUBACK — completes a subscribe waiter in the write task.
    ///
    /// `generation` is the read-task connection generation that observed the frame;
    /// stale-generation ACKs are discarded, same as [`Self::PublishAck`].
    SubscribeAck {
        /// Decoded SUBACK packet.
        ack: SubscribeAck,
        /// Connection generation of the read task that received this SUBACK.
        generation: Generation,
    },
    /// Broker UNSUBACK — completes an unsubscribe waiter in the write task.
    ///
    /// `generation` is the read-task connection generation that observed the frame;
    /// stale-generation ACKs are discarded, same as [`Self::SubscribeAck`].
    UnsubscribeAck {
        /// Decoded UNSUBACK packet.
        ack: UnsubscribeAck,
        /// Connection generation of the read task that received this UNSUBACK.
        generation: Generation,
    },
    /// Broker PUBACK — completes an outbound QoS-1 publish waiter.
    ///
    /// `generation` is the read-task connection generation that observed the frame;
    /// stale-generation ACKs are discarded.
    PublishAck {
        /// Decoded PUBACK packet.
        ack: PublishAck,
        /// Connection generation of the read task that received this PUBACK.
        generation: Generation,
    },
    /// Client → broker PUBACK for an inbound QoS-1 PUBLISH after successful handoff accept.
    AckInboundPublish {
        /// Packet identifier from the inbound PUBLISH.
        packet_id: std::num::NonZeroU16,
    },
    /// Test-only hook: no-op control variant (read exit uses the stub-exit mpsc).
    #[cfg(test)]
    #[doc(hidden)]
    #[expect(dead_code, reason = "test-only variant; read exit uses stub-exit mpsc")]
    StubExitRead,
    /// Test-only hook: causes the write task to exit so supervision can be verified.
    #[cfg(test)]
    #[doc(hidden)]
    #[expect(dead_code, reason = "test-only variant; supervision tests use stub-exit mpsc")]
    StubExitWrite,
}

/// Reply channel for [`ClientCommand::Subscribe`].
pub(crate) type SubscribeReply = oneshot::Sender<Result<crate::ack::SubscribeResponse, AppError>>;

/// Reply channel for [`ClientCommand::Unsubscribe`].
pub(crate) type UnsubscribeReply = oneshot::Sender<Result<crate::ack::UnsubscribeResponse, AppError>>;

/// Completion value of a publish, sent by the write task.
pub(crate) type PublishResult = Result<crate::ack::PublishResponse, AppError>;

const QUEUED: u8 = 0;
const WRITTEN: u8 = 1;
const ABANDONED: u8 = 2;

/// Whether a QoS 1 publish has been written, shared by its [`PublishReply`] (write task) and
/// [`PublishTicket`] (caller): `Queued -> Written` by the write task, `Queued -> Abandoned`
/// by a caller that stops waiting. Whichever switch wins decides whether a timed-out publish
/// was sent, so the caller's answer cannot race the write.
///
/// QoS 0 goes without it: the extra allocation and shared cache line would cost QoS 0 a
/// measurable share of its throughput, and a QoS 0 caller rarely acts on the difference.
#[derive(Debug, Default)]
struct WriteState(AtomicU8);

/// Reply side of [`ClientCommand::Publish`], held by the write task.
#[derive(Debug)]
pub(crate) struct PublishReply {
    tx: oneshot::Sender<PublishResult>,
    state: Option<Arc<WriteState>>,
}

/// Caller side of [`ClientCommand::Publish`].
#[derive(Debug)]
pub(crate) struct PublishTicket {
    pub(crate) rx: oneshot::Receiver<PublishResult>,
    state: Option<Arc<WriteState>>,
}

/// What a caller that stopped waiting learns about its publish ([`PublishTicket::abandon`]).
#[derive(Debug)]
pub(crate) enum Abandoned {
    /// Not written yet, and now it never will be.
    NotWritten,
    /// Written already, or possibly being written (QoS 0 keeps no write state).
    MaybeWritten,
    /// The result arrived meanwhile after all.
    Completed(PublishResult),
}

/// Creates the two ends for one publish; `track_write` keeps the write state (QoS 1).
pub(crate) fn publish_reply(track_write: bool) -> (PublishReply, PublishTicket) {
    let (tx, rx) = oneshot::channel();
    let state = track_write.then(|| Arc::new(WriteState::default()));
    (
        PublishReply {
            tx,
            state: state.clone(),
        },
        PublishTicket { rx, state },
    )
}

impl PublishReply {
    /// Whether the caller stopped waiting (gave up or dropped its completion).
    #[inline]
    pub(crate) fn is_closed(&self) -> bool {
        self.tx.is_closed()
            || self
                .state
                .as_ref()
                .is_some_and(|state| state.0.load(Ordering::Acquire) == ABANDONED)
    }

    /// Claims the publish for writing, right before it is encoded. `false` when the caller
    /// gave up first: it has been told the publish was not sent, so it must not be written.
    #[inline]
    pub(crate) fn claim_write(&self) -> bool {
        if self.tx.is_closed() {
            return false;
        }
        let Some(state) = &self.state else {
            return true;
        };
        match state
            .0
            .compare_exchange(QUEUED, WRITTEN, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(current) => current == WRITTEN,
        }
    }

    /// Undoes [`Self::claim_write`] when encoding failed before any byte was written.
    #[inline]
    pub(crate) fn unclaim_write(&self) {
        if let Some(state) = &self.state {
            let _ = state
                .0
                .compare_exchange(WRITTEN, QUEUED, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    /// Completes the publish; the caller may be gone already.
    #[inline]
    pub(crate) fn send(self, result: PublishResult) {
        let _ = self.tx.send(result);
    }
}

impl PublishTicket {
    /// Stops waiting after a timeout. A publish not yet written is never written afterwards.
    pub(crate) fn abandon(&mut self) -> Abandoned {
        if let Some(state) = &self.state {
            return match state
                .0
                .compare_exchange(QUEUED, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => Abandoned::NotWritten,
                Err(_) => Abandoned::MaybeWritten,
            };
        }
        // QoS 0 replies right when it writes: closing first means the write task skips it
        // from now on, but one being written at this very moment cannot be told apart.
        self.rx.close();
        match self.rx.try_recv() {
            Ok(result) => Abandoned::Completed(result),
            Err(_) => Abandoned::MaybeWritten,
        }
    }

    /// Whether the publish was written to a connection. Always `false` for QoS 0: it
    /// completes with `Ok` when written, so an error means it was not.
    pub(crate) fn written(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.0.load(Ordering::Acquire) == WRITTEN)
    }
}

/// Application commands routed to the write task over a bounded channel.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ClientCommand {
    /// No-op used by tests to fill the command channel.
    #[cfg(test)]
    #[doc(hidden)]
    Noop,
    /// Test hook: enter graceful shutdown through the command channel (production uses the
    /// shutdown token, which a full channel cannot delay).
    #[cfg(test)]
    Shutdown,
    /// Outbound PUBLISH; QoS 0 completes after encode, QoS 1 after PUBACK.
    Publish {
        /// Application message (codec [`PublishMessage`]).
        message: PublishMessage,
        /// Delivery QoS (0 or 1; QoS 2 is rejected by the driver).
        qos: QoS,
        /// MQTT Retain flag.
        retain: bool,
        /// Oneshot completed on success or a typed error.
        reply: PublishReply,
    },
    /// Outbound SUBSCRIBE; write allocates a packet id and completes `reply` on SUBACK.
    Subscribe {
        /// Topic filters and per-filter options.
        filters: Vec<(ByteString, SubscriptionOptions)>,
        /// Subscription Identifier for the whole SUBSCRIBE (§3.8.2.1.2).
        id: Option<std::num::NonZeroU32>,
        /// Oneshot completed with per-filter SUBACK reason codes (or a typed error).
        reply: SubscribeReply,
    },
    /// Outbound UNSUBSCRIBE; write allocates a packet id and completes `reply` on UNSUBACK.
    Unsubscribe {
        /// Topic filters to remove.
        filters: Vec<ByteString>,
        /// Oneshot completed with per-filter UNSUBACK reason codes (or a typed error).
        reply: UnsubscribeReply,
    },
    /// Test-only: insert a QoS-1 publish waiter without encoding (generation / late-ACK tests).
    ///
    /// Does **not** allocate from the inflight pot — not the production packet-ID ownership model.
    #[cfg(test)]
    #[doc(hidden)]
    InsertPublishWaiter {
        /// Packet identifier the subsequent PUBACK will target.
        packet_id: std::num::NonZeroU16,
        /// Oneshot completed only by a matching current-generation PUBACK.
        reply: PublishReply,
    },
    /// Test-only: insert a subscribe waiter without encoding (generation / late-ACK tests).
    ///
    /// Does **not** allocate from the inflight pot — not the production packet-ID ownership model.
    #[cfg(test)]
    #[doc(hidden)]
    InsertSubscribeWaiter {
        /// Packet identifier the subsequent SUBACK will target.
        packet_id: std::num::NonZeroU16,
        /// Oneshot completed only by a matching current-generation SUBACK.
        reply: SubscribeReply,
    },
    /// Test-only: insert an unsubscribe waiter without encoding (generation / late-ACK tests).
    ///
    /// Does **not** allocate from the inflight pot — not the production packet-ID ownership model.
    #[cfg(test)]
    #[doc(hidden)]
    InsertUnsubscribeWaiter {
        /// Packet identifier the subsequent UNSUBACK will target.
        packet_id: std::num::NonZeroU16,
        /// Oneshot completed only by a matching current-generation UNSUBACK.
        reply: UnsubscribeReply,
    },
}

/// Bounded control channel sender type alias.
pub(crate) type ControlSender = mpsc::Sender<ControlMessage>;

/// Bounded control channel receiver type alias.
pub(crate) type ControlReceiver = mpsc::Receiver<ControlMessage>;

/// Bounded command channel sender type alias.
pub(crate) type CommandSender = mpsc::Sender<ClientCommand>;

/// Bounded command channel receiver type alias.
pub(crate) type CommandReceiver = mpsc::Receiver<ClientCommand>;

/// Fails what is still queued once nothing will send it: publishes with
/// [`AppError::ConnectionLost`], like the unacknowledged ones, and subscribe / unsubscribe
/// with `refused`. Dropping them instead would hand every caller
/// [`AppError::CommandChannelClosed`], which reads as a client that is gone.
pub(crate) fn fail_queued_commands(command_rx: &mut CommandReceiver, refused: &AppError) {
    command_rx.close();
    while let Ok(command) = command_rx.try_recv() {
        match command {
            ClientCommand::Publish { reply, .. } => {
                reply.send(Err(AppError::ConnectionLost));
            }
            ClientCommand::Subscribe { reply, .. } => {
                let _ = reply.send(Err(refused.clone()));
            }
            ClientCommand::Unsubscribe { reply, .. } => {
                let _ = reply.send(Err(refused.clone()));
            }
            #[allow(unreachable_patterns)] // test-only commands
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

    /// The control channel holds `max(128, receive_maximum)`.
    #[rstest::rstest]
    #[case::below_floor(nonzero!(32u16), 128)]
    #[case::at_floor(nonzero!(128u16), 128)]
    #[case::above_floor(nonzero!(256u16), 256)]
    #[case::large(nonzero!(1024u16), 1024)]
    fn control_capacity_is_receive_maximum_floored_at_128(
        #[case] receive_maximum: NonZeroU16,
        #[case] expected: usize,
    ) {
        assert_eq!(control_channel_capacity(receive_maximum), expected);
    }
}
