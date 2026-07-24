//! Scenario matrix cells.

use super::fixtures::{parse_ack_policies, parse_directions, parse_payloads, parse_qos_list};
use super::harness::{AckPolicy, BenchQos};

/// What to measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Sustained ingress load (publisher subprocess).
    IngressLoad,
    /// Sustained outbound serial (await each completion).
    OutboundSerial,
    /// Sustained outbound pipelined (`LOAD_WINDOW`).
    OutboundPipelined,
    /// Sustained outbound fan-out: `LOAD_CONCURRENCY` tasks each calling `publish_serial`.
    OutboundConcurrent,
    /// Ingress serial latency (p50/p99).
    IngressSerial,
    /// Outbound serial latency (p50/p99).
    OutboundSerialLatency,
}

impl Direction {
    /// JSON / file label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IngressLoad => "ingress_load",
            Self::OutboundSerial => "outbound_serial",
            Self::OutboundPipelined => "outbound_pipelined",
            Self::OutboundConcurrent => "outbound_concurrent",
            Self::IngressSerial => "ingress_serial",
            Self::OutboundSerialLatency => "outbound_serial_latency",
        }
    }

    /// Whether this direction uses ingress ack policy (QoS 1 only).
    pub const fn uses_ingress_ack(self) -> bool {
        matches!(self, Self::IngressLoad | Self::IngressSerial)
    }
}

/// One absolute measurement cell (before client filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioCell {
    /// Direction.
    pub direction: Direction,
    /// QoS.
    pub qos: BenchQos,
    /// Payload size in bytes.
    pub payload: usize,
    /// Ingress ack policy (only meaningful for ingress + QoS 1).
    pub ack: Option<AckPolicy>,
}

/// Builds the Cartesian product from env filters.
pub fn build_matrix() -> Vec<ScenarioCell> {
    let directions = parse_directions();
    let qos_list = parse_qos_list();
    let payloads = parse_payloads();
    let acks = parse_ack_policies();
    let mut cells = Vec::new();
    for direction in directions {
        for qos in &qos_list {
            for &payload in &payloads {
                if direction.uses_ingress_ack() && *qos == BenchQos::AtLeastOnce {
                    for &ack in &acks {
                        cells.push(ScenarioCell {
                            direction,
                            qos: *qos,
                            payload,
                            ack: Some(ack),
                        });
                    }
                } else {
                    cells.push(ScenarioCell {
                        direction,
                        qos: *qos,
                        payload,
                        ack: None,
                    });
                }
            }
        }
    }
    cells
}
