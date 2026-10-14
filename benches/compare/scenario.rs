//! Scenario matrix cells.

use super::fixtures::{
    parse_ack_policies, parse_concurrency, parse_directions, parse_payloads, parse_qos_list, parse_rates,
};
use super::harness::{AckPolicy, BenchQos};

/// What to measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Sustained ingress load (publisher subprocess).
    IngressLoad,
    /// Sustained outbound pipelined (`LOAD_WINDOW`) from one task: bulk / batch export.
    OutboundPipelined,
    /// Shared client, `level` long-lived caller tasks each awaiting every publish
    /// (a service handling requests). Reports throughput, CPU and per-publish latency.
    OutboundConcurrent,
    /// One task spawned per message, awaiting its publish (fire-and-forget relays).
    /// Throughput and CPU only.
    OutboundSpawn,
    /// Open loop: `level` msg/s released on a fixed schedule; the generator never waits on
    /// the client, so falling behind shows up as latency and a lower achieved rate.
    OutboundRate,
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
            Self::OutboundPipelined => "outbound_pipelined",
            Self::OutboundConcurrent => "outbound_concurrent",
            Self::OutboundSpawn => "outbound_spawn",
            Self::OutboundRate => "outbound_rate",
            Self::IngressSerial => "ingress_serial",
            Self::OutboundSerialLatency => "outbound_serial_latency",
        }
    }

    /// Whether this direction uses ingress ack policy (QoS 1 only).
    pub const fn uses_ingress_ack(self) -> bool {
        matches!(self, Self::IngressLoad | Self::IngressSerial)
    }

    /// Whether this is a client → broker scenario.
    pub const fn is_outbound(self) -> bool {
        matches!(
            self,
            Self::OutboundPipelined
                | Self::OutboundConcurrent
                | Self::OutboundSpawn
                | Self::OutboundRate
                | Self::OutboundSerialLatency
        )
    }

    /// Per-cell levels: caller tasks (concurrent), offered msg/s (rate), else none.
    fn levels(self) -> Vec<Option<u32>> {
        match self {
            Self::OutboundConcurrent => parse_concurrency().into_iter().map(Some).collect(),
            Self::OutboundRate => parse_rates().into_iter().map(Some).collect(),
            _ => vec![None],
        }
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
    /// Caller tasks ([`Direction::OutboundConcurrent`]) or offered msg/s ([`Direction::OutboundRate`]).
    pub level: Option<u32>,
}

/// Builds the Cartesian product from env filters.
pub fn build_matrix() -> Vec<ScenarioCell> {
    let directions = parse_directions();
    let qos_list = parse_qos_list();
    let payloads = parse_payloads();
    let acks = parse_ack_policies();
    let mut cells = Vec::new();
    for direction in directions {
        for level in direction.levels() {
            for qos in &qos_list {
                for &payload in &payloads {
                    let cell = |ack| ScenarioCell {
                        direction,
                        qos: *qos,
                        payload,
                        ack,
                        level,
                    };
                    if direction.uses_ingress_ack() && *qos == BenchQos::AtLeastOnce {
                        cells.extend(acks.iter().map(|&ack| cell(Some(ack))));
                    } else {
                        cells.push(cell(None));
                    }
                }
            }
        }
    }
    cells
}
