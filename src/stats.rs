//! Client counters behind [`MqttClient::stats`](crate::MqttClient::stats) (`stats` feature).
//!
//! The connection's tasks keep [`Stats`] up to date. Without the feature it is a zero-sized
//! type whose methods do nothing, so the call sites stay in place and compile away.

#[cfg(feature = "stats")]
pub(crate) use imp::Stats;
#[cfg(feature = "stats")]
pub use imp::{ClientStats, ConnectionStats, InboundStats, OutboundStats};
#[cfg(not(feature = "stats"))]
pub(crate) use noop::Stats;

#[cfg(feature = "stats")]
mod imp {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use crate::codec::{PublishAckReason, QoS};

    /// A snapshot of what a client's connection has done since [`connect`](crate::connect),
    /// across reconnects.
    ///
    /// Counters only grow; the fields documented as "right now" are current values. The
    /// snapshot is not taken atomically across fields, so two related counters read while
    /// traffic flows can be a message apart. Export it to whatever metrics system the
    /// application uses; see [`MqttClient::stats`](crate::MqttClient::stats).
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct ClientStats {
        /// Connection attempts, sessions and keep-alive.
        pub connection: ConnectionStats,
        /// What this client sent.
        pub outbound: OutboundStats,
        /// What the broker sent to this client.
        pub inbound: InboundStats,
    }

    /// Connection counters of a [`ClientStats`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct ConnectionStats {
        /// Sessions established (a successful CONNACK and, after a reconnect, restored
        /// subscriptions). Every one after the first is a reconnect.
        pub connects: u64,
        /// Connection attempts that ended before a session was up: dial, TLS or proxy
        /// failures, a refused CONNACK, a timeout.
        pub connect_failures: u64,
        /// Established sessions that ended other than by [`MqttClient::shutdown`](crate::MqttClient::shutdown).
        pub connection_losses: u64,
        /// PINGREQs sent; only an otherwise quiet connection needs them.
        pub pings_sent: u64,
    }

    /// Outbound counters of a [`ClientStats`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct OutboundStats {
        /// QoS 0 PUBLISHes written to the connection.
        pub publishes_qos0: u64,
        /// QoS 1 PUBLISHes written to the connection, first transmissions only.
        pub publishes_qos1: u64,
        /// Unacknowledged QoS 1 PUBLISHes sent again (DUP) after a reconnect resumed the session.
        pub publishes_resent: u64,
        /// QoS 1 PUBLISHes the broker accepted (PUBACK Success or No Matching Subscribers).
        pub publishes_acked: u64,
        /// QoS 1 PUBLISHes the broker refused (a PUBACK reason code of 0x80 or above).
        pub publishes_rejected: u64,
        /// PUBACKs that arrived after the caller had stopped waiting: its acknowledgement
        /// timeout ran out (the publish was reported as
        /// [`Delivery::Unknown`](crate::Delivery::Unknown)), or it dropped the completion.
        /// Included in `publishes_acked` / `publishes_rejected`. A steady count means the
        /// acknowledgement timeout is shorter than the broker's answers take.
        pub late_acks: u64,
        /// QoS 1 PUBLISHes on the wire waiting for their PUBACK, right now. Stays at the
        /// broker's Receive Maximum while the broker is the bottleneck.
        pub awaiting_ack: u64,
        /// QoS 1 PUBLISHes held back until the broker's Receive Maximum frees a slot, right now.
        pub awaiting_quota: u64,
        /// Commands (publishes, subscribes, …) queued for the connection and not yet taken
        /// by it, right now. Near the command channel capacity, callers start to wait.
        pub commands_queued: u64,
        /// Bytes of MQTT packets written to the broker connection (without TLS records or
        /// WebSocket framing).
        pub bytes: u64,
    }

    /// Inbound counters of a [`ClientStats`].
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    #[non_exhaustive]
    pub struct InboundStats {
        /// QoS 0 PUBLISHes received.
        pub publishes_qos0: u64,
        /// QoS 1 PUBLISHes received.
        pub publishes_qos1: u64,
        /// Received PUBLISHes with the DUP flag: the broker redelivers one it may have sent before.
        pub duplicates: u64,
        /// Times a received PUBLISH found the inbound sink full and the connection waited for
        /// the application to take messages (the broker is held back meanwhile).
        pub handoff_stalls: u64,
        /// Total time spent in those waits.
        pub handoff_stall_time: Duration,
        /// Bytes of MQTT packets read from the broker connection (without TLS records or
        /// WebSocket framing).
        pub bytes: u64,
    }

    /// The counters, shared by the client handles and the connection's tasks.
    ///
    /// Every counter has one writer at a time: the write side's are only touched by the
    /// session's write task, the read side's by its read task (or, before those start, by the
    /// connection driver doing the handshake), the session counters by the driver. A session's
    /// tasks are joined before the next session starts. So an increment is a plain load and
    /// store, not a locked read-modify-write; readers see each value whole.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct Stats(Arc<Counters>);

    #[derive(Debug, Default)]
    struct Counters {
        write: CacheLine<WriteCounters>,
        read: CacheLine<ReadCounters>,
        session: CacheLine<SessionCounters>,
    }

    /// Keeps each writer's counters off the others' cache lines.
    #[derive(Debug, Default)]
    #[repr(align(128))]
    struct CacheLine<T>(T);

    #[derive(Debug, Default)]
    struct WriteCounters {
        pings: AtomicU64,
        publishes_qos0: AtomicU64,
        publishes_qos1: AtomicU64,
        publishes_resent: AtomicU64,
        publishes_acked: AtomicU64,
        publishes_rejected: AtomicU64,
        late_acks: AtomicU64,
        awaiting_ack: AtomicU64,
        awaiting_quota: AtomicU64,
        bytes: AtomicU64,
    }

    #[derive(Debug, Default)]
    struct ReadCounters {
        publishes_qos0: AtomicU64,
        publishes_qos1: AtomicU64,
        duplicates: AtomicU64,
        handoff_stalls: AtomicU64,
        handoff_stall_nanos: AtomicU64,
        bytes: AtomicU64,
    }

    #[derive(Debug, Default)]
    struct SessionCounters {
        connects: AtomicU64,
        connect_failures: AtomicU64,
        connection_losses: AtomicU64,
    }

    /// Single-writer increment (see [`Stats`]).
    #[inline(always)]
    fn add(counter: &AtomicU64, n: u64) {
        counter.store(counter.load(Ordering::Relaxed).wrapping_add(n), Ordering::Relaxed);
    }

    #[inline(always)]
    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    impl Stats {
        /// Snapshot; `commands_queued` comes from the caller, who holds the command channel.
        pub(crate) fn snapshot(&self, commands_queued: usize) -> ClientStats {
            let write = &self.0.write.0;
            let read = &self.0.read.0;
            let session = &self.0.session.0;
            ClientStats {
                connection: ConnectionStats {
                    connects: get(&session.connects),
                    connect_failures: get(&session.connect_failures),
                    connection_losses: get(&session.connection_losses),
                    pings_sent: get(&write.pings),
                },
                outbound: OutboundStats {
                    publishes_qos0: get(&write.publishes_qos0),
                    publishes_qos1: get(&write.publishes_qos1),
                    publishes_resent: get(&write.publishes_resent),
                    publishes_acked: get(&write.publishes_acked),
                    publishes_rejected: get(&write.publishes_rejected),
                    late_acks: get(&write.late_acks),
                    awaiting_ack: get(&write.awaiting_ack),
                    awaiting_quota: get(&write.awaiting_quota),
                    commands_queued: commands_queued as u64,
                    bytes: get(&write.bytes),
                },
                inbound: InboundStats {
                    publishes_qos0: get(&read.publishes_qos0),
                    publishes_qos1: get(&read.publishes_qos1),
                    duplicates: get(&read.duplicates),
                    handoff_stalls: get(&read.handoff_stalls),
                    handoff_stall_time: Duration::from_nanos(get(&read.handoff_stall_nanos)),
                    bytes: get(&read.bytes),
                },
            }
        }

        // Driver.

        #[inline]
        pub(crate) fn session_up(&self) {
            add(&self.0.session.0.connects, 1);
        }

        #[inline]
        pub(crate) fn session_ended(&self, was_up: bool) {
            let session = &self.0.session.0;
            add(
                if was_up {
                    &session.connection_losses
                } else {
                    &session.connect_failures
                },
                1,
            );
        }

        // Write task.

        #[inline]
        pub(crate) fn ping_sent(&self) {
            add(&self.0.write.0.pings, 1);
        }

        #[inline]
        pub(crate) fn publish_sent(&self, qos: QoS) {
            let write = &self.0.write.0;
            add(
                if qos == QoS::AtMostOnce {
                    &write.publishes_qos0
                } else {
                    &write.publishes_qos1
                },
                1,
            );
        }

        #[inline]
        pub(crate) fn publish_resent(&self) {
            add(&self.0.write.0.publishes_resent, 1);
        }

        #[inline]
        pub(crate) fn publish_ack(&self, reason: PublishAckReason) {
            let write = &self.0.write.0;
            let accepted = matches!(
                reason,
                PublishAckReason::Success | PublishAckReason::NoMatchingSubscribers
            );
            add(
                if accepted {
                    &write.publishes_acked
                } else {
                    &write.publishes_rejected
                },
                1,
            );
        }

        /// A PUBACK for a publish whose caller had stopped waiting.
        #[inline]
        pub(crate) fn late_ack(&self) {
            add(&self.0.write.0.late_acks, 1);
        }

        /// Current window state of the write task (zero once the session ended).
        #[inline]
        pub(crate) fn outbound_window(&self, awaiting_ack: usize, awaiting_quota: usize) {
            let write = &self.0.write.0;
            write.awaiting_ack.store(awaiting_ack as u64, Ordering::Relaxed);
            write.awaiting_quota.store(awaiting_quota as u64, Ordering::Relaxed);
        }

        // Read task.

        #[inline]
        pub(crate) fn publish_received(&self, qos: QoS, dup: bool) {
            let read = &self.0.read.0;
            add(
                if qos == QoS::AtMostOnce {
                    &read.publishes_qos0
                } else {
                    &read.publishes_qos1
                },
                1,
            );
            if dup {
                add(&read.duplicates, 1);
            }
        }

        /// Starts timing a wait on a full inbound sink; the wait ends when the guard drops.
        #[inline]
        pub(crate) fn handoff_stall(&self) -> impl Drop + '_ {
            struct StallTimer<'a> {
                read: &'a ReadCounters,
                since: std::time::Instant,
            }
            impl Drop for StallTimer<'_> {
                fn drop(&mut self) {
                    let nanos = u64::try_from(self.since.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    add(&self.read.handoff_stall_nanos, nanos);
                }
            }
            add(&self.0.read.0.handoff_stalls, 1);
            StallTimer {
                read: &self.0.read.0,
                since: std::time::Instant::now(),
            }
        }

        // Transport.

        /// Counts the bytes read from `reader` as inbound.
        pub(crate) fn count_reads<R>(&self, reader: R) -> Counted<R> {
            Counted {
                inner: reader,
                stats: self.clone(),
            }
        }

        /// Counts the bytes written to `writer` as outbound.
        pub(crate) fn count_writes<W>(&self, writer: W) -> Counted<W> {
            Counted {
                inner: writer,
                stats: self.clone(),
            }
        }
    }

    /// A connection half that adds what passes through it to [`Stats`].
    #[derive(Debug)]
    pub(crate) struct Counted<T> {
        inner: T,
        stats: Stats,
    }

    impl<R: AsyncRead + Unpin> AsyncRead for Counted<R> {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = polled {
                add(&self.stats.0.read.0.bytes, (buf.filled().len() - before) as u64);
            }
            polled
        }
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for Counted<W> {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
            if let Poll::Ready(Ok(written)) = polled {
                add(&self.stats.0.write.0.bytes, written as u64);
            }
            polled
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
            if let Poll::Ready(Ok(written)) = polled {
                add(&self.stats.0.write.0.bytes, written as u64);
            }
            polled
        }

        fn is_write_vectored(&self) -> bool {
            self.inner.is_write_vectored()
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
}

#[cfg(not(feature = "stats"))]
mod noop {
    use crate::codec::{PublishAckReason, QoS};

    /// Stand-in without the `stats` feature: zero-sized, every method a no-op.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct Stats(());

    /// What [`Stats::handoff_stall`] hands out: nothing to time.
    pub(crate) struct NoTimer;

    impl Stats {
        #[inline(always)]
        pub(crate) fn session_up(&self) {}
        #[inline(always)]
        pub(crate) fn session_ended(&self, _was_up: bool) {}
        #[inline(always)]
        pub(crate) fn ping_sent(&self) {}
        #[inline(always)]
        pub(crate) fn publish_sent(&self, _qos: QoS) {}
        #[inline(always)]
        pub(crate) fn publish_resent(&self) {}
        #[inline(always)]
        pub(crate) fn publish_ack(&self, _reason: PublishAckReason) {}
        #[inline(always)]
        pub(crate) fn late_ack(&self) {}
        #[inline(always)]
        pub(crate) fn outbound_window(&self, _awaiting_ack: usize, _awaiting_quota: usize) {}
        #[inline(always)]
        pub(crate) fn publish_received(&self, _qos: QoS, _dup: bool) {}
        #[inline(always)]
        pub(crate) fn handoff_stall(&self) -> NoTimer {
            NoTimer
        }
        #[inline(always)]
        pub(crate) fn count_reads<R>(&self, reader: R) -> R {
            reader
        }
        #[inline(always)]
        pub(crate) fn count_writes<W>(&self, writer: W) -> W {
            writer
        }
    }
}
