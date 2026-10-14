//! Server redirection (§4.11): a CONNACK or DISCONNECT with Reason Code 0x9C (Use another
//! server) or 0x9D (Server moved) plus a Server Reference names where to connect instead.

use bytestring::ByteString;

use crate::codec::{ConnectAck, ConnectAckReason, Disconnect, DisconnectReasonCode};

/// Redirects followed in a row before the client falls back to its configured broker and
/// reconnect policy, so two brokers pointing at each other cannot keep it bouncing.
pub(crate) const MAX_REDIRECTS_IN_ROW: u8 = 5;

/// A broker's request to connect elsewhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerRedirect {
    /// The Server Reference as sent (format unspecified by MQTT, see [`crate::BrokerUrl`]).
    pub(crate) reference: ByteString,
    /// 0x9D Server moved: use the new server from now on. 0x9C: only for the next connection.
    pub(crate) permanent: bool,
}

impl ServerRedirect {
    /// From a refused CONNACK; `None` unless it is a redirect that names a server.
    pub(crate) fn from_connack(ack: &ConnectAck) -> Option<Self> {
        let permanent = match ack.reason_code {
            ConnectAckReason::UseAnotherServer => false,
            ConnectAckReason::ServerMoved => true,
            _ => return None,
        };
        Some(Self {
            reference: ack.server_reference.clone()?,
            permanent,
        })
    }

    /// From a broker DISCONNECT; `None` unless it is a redirect that names a server.
    pub(crate) fn from_disconnect(disconnect: &Disconnect) -> Option<Self> {
        let permanent = match disconnect.reason_code {
            DisconnectReasonCode::UseAnotherServer => false,
            DisconnectReasonCode::ServerMoved => true,
            _ => return None,
        };
        Some(Self {
            reference: disconnect.server_reference.clone()?,
            permanent,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_redirect_codes_with_a_reference_redirect() {
        let mut ack = ConnectAck {
            reason_code: ConnectAckReason::UseAnotherServer,
            server_reference: Some("b.example.com".into()),
            ..ConnectAck::default()
        };
        assert_eq!(
            ServerRedirect::from_connack(&ack),
            Some(ServerRedirect {
                reference: "b.example.com".into(),
                permanent: false
            })
        );
        ack.reason_code = ConnectAckReason::ServerMoved;
        assert!(ServerRedirect::from_connack(&ack).expect("moved").permanent);
        ack.reason_code = ConnectAckReason::ServerUnavailable;
        assert_eq!(
            ServerRedirect::from_connack(&ack),
            None,
            "a reference alone is no redirect"
        );
        ack.reason_code = ConnectAckReason::UseAnotherServer;
        ack.server_reference = None;
        assert_eq!(ServerRedirect::from_connack(&ack), None, "nowhere to go");

        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::ServerMoved,
            server_reference: Some("c.example.com:8883".into()),
            ..Disconnect::default()
        };
        assert!(ServerRedirect::from_disconnect(&disconnect).expect("moved").permanent);
    }
}
