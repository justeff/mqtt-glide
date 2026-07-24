//! Per-connection channel types and capacities.

use std::num::NonZeroU16;

use bytestring::ByteString;
use tokio::sync::{mpsc, oneshot};

use crate::codec::{
    PublishAck, PublishMessage, QoS, SubscribeAck, SubscribeAckReason, SubscriptionOptions, UnsubscribeAck,
    UnsubscribeAckReason,
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
    /// Protocol-error disconnect (Reason Code 0x82) queued by the read task.
    ProtocolDisconnect,
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
pub(crate) type SubscribeReply = oneshot::Sender<Result<Vec<SubscribeAckReason>, AppError>>;

/// Reply channel for [`ClientCommand::Unsubscribe`].
pub(crate) type UnsubscribeReply = oneshot::Sender<Result<Vec<UnsubscribeAckReason>, AppError>>;

/// Reply channel for [`ClientCommand::Publish`].
pub(crate) type PublishReply = oneshot::Sender<Result<(), AppError>>;

/// Application commands routed to the write task over a bounded channel.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ClientCommand {
    /// No-op used by tests to fill the command channel.
    #[cfg(test)]
    #[doc(hidden)]
    Noop,
    /// Request graceful shutdown of the connection driver.
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

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn control_capacity_floors_at_128() {
        assert_eq!(control_channel_capacity(nonzero!(32u16)), 128);
        assert_eq!(control_channel_capacity(nonzero!(128u16)), 128);
    }

    #[test]
    fn control_capacity_tracks_larger_receive_maximum() {
        assert_eq!(control_channel_capacity(nonzero!(256u16)), 256);
        assert_eq!(control_channel_capacity(nonzero!(1024u16)), 1024);
    }
}
