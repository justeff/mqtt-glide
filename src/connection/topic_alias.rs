//! Per-connection Topic Alias state (§3.3.2.3.4).
//!
//! Aliases only live as long as one network connection; both maps are created fresh for
//! every connection, so nothing here survives a reconnect ([MQTT-3.3.2-7]).

use std::collections::HashMap;
use std::num::NonZeroU16;

use bytestring::ByteString;

use crate::codec::{OutboundTopicAlias, PublishPacket};
use crate::error::ProtocolError;

/// Resolves Topic Aliases the broker uses towards us (bounded by the Topic Alias Maximum we
/// advertised in CONNECT).
#[derive(Debug, Clone)]
pub(crate) struct InboundTopicAliases {
    /// Index = alias; index 0 unused.
    topics: Vec<Option<ByteString>>,
}

impl InboundTopicAliases {
    pub(crate) fn new(maximum: u16) -> Self {
        Self {
            topics: vec![None; usize::from(maximum) + 1],
        }
    }

    fn maximum(&self) -> usize {
        self.topics.len() - 1
    }

    /// Replaces an aliased PUBLISH's topic with the bound one (or records a new binding) and
    /// clears the wire alias. Alias 0, an alias above our maximum, or an unbound alias with
    /// an empty topic are protocol errors ([MQTT-3.3.2-7], [MQTT-3.3.2-9], [MQTT-3.3.4-7]).
    pub(crate) fn resolve(&mut self, packet: &mut PublishPacket) -> Result<(), ProtocolError> {
        let Some(alias) = packet.wire_topic_alias.take() else {
            return Ok(());
        };
        let index = usize::from(alias.get());
        if index > self.maximum() {
            return Err(ProtocolError::TopicAliasNotSupported);
        }
        if packet.message.topic.is_empty() {
            let topic = self.topics[index]
                .as_ref()
                .ok_or_else(|| ProtocolError::Violation(format!("PUBLISH used unbound Topic Alias {alias}")))?;
            packet.message.topic = topic.clone();
        } else {
            self.topics[index] = Some(packet.message.topic.clone());
        }
        Ok(())
    }
}

/// Chooses Topic Aliases for outbound PUBLISHes: at most `capacity` topics are bound, the
/// least recently used one is rebound when all aliases are taken.
///
/// [`Self::choose`] only proposes; [`Self::commit`] records the binding once the PUBLISH is
/// actually encoded, so a rejected publish can never leave an alias the broker has not
/// seen bound locally.
#[derive(Debug, Default)]
pub(crate) struct OutboundTopicAliases {
    by_topic: HashMap<ByteString, NonZeroU16>,
    /// Index = alias - 1: bound topic and last use.
    by_alias: Vec<Option<(ByteString, u64)>>,
    capacity: u16,
    clock: u64,
}

impl OutboundTopicAliases {
    /// `capacity` = min(our configured limit, broker's CONNACK Topic Alias Maximum); 0 disables.
    pub(crate) fn new(capacity: u16) -> Self {
        Self {
            capacity,
            ..Self::default()
        }
    }

    /// Whether this connection uses outbound aliases at all.
    #[inline]
    pub(crate) fn enabled(&self) -> bool {
        self.capacity != 0
    }

    /// Alias to use for `topic`, if aliasing is enabled.
    #[inline]
    pub(crate) fn choose(&self, topic: &ByteString) -> Option<OutboundTopicAlias> {
        if self.capacity == 0 {
            return None;
        }
        if let Some(alias) = self.by_topic.get(topic) {
            return Some(OutboundTopicAlias::Reuse(*alias));
        }
        let alias = if self.by_alias.len() < usize::from(self.capacity) {
            self.by_alias.len() + 1
        } else {
            // Least recently used binding gets rebound.
            self.by_alias
                .iter()
                .enumerate()
                .min_by_key(|(_, slot)| slot.as_ref().map_or(0, |(_, used)| *used))
                .map(|(index, _)| index + 1)
                .expect("capacity > 0")
        };
        let alias = u16::try_from(alias)
            .ok()
            .and_then(NonZeroU16::new)
            .expect("alias within u16");
        Some(OutboundTopicAlias::Assign(alias))
    }

    /// Records the outcome of [`Self::choose`] after a successful encode.
    pub(crate) fn commit(&mut self, topic: &ByteString, alias: OutboundTopicAlias) {
        self.clock += 1;
        let index = match alias {
            OutboundTopicAlias::Reuse(alias) | OutboundTopicAlias::Assign(alias) => usize::from(alias.get()) - 1,
        };
        if let OutboundTopicAlias::Assign(alias) = alias {
            if index == self.by_alias.len() {
                self.by_alias.push(None);
            }
            if let Some((previous, _)) = self.by_alias[index].take() {
                self.by_topic.remove(&previous);
            }
            self.by_topic.insert(topic.clone(), alias);
            self.by_alias[index] = Some((topic.clone(), self.clock));
        } else if let Some((_, used)) = self.by_alias[index].as_mut() {
            *used = self.clock;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::codec::{PublishMessage, PublishPacketMeta, QoS};

    fn packet(topic: &'static str, alias: Option<u16>) -> PublishPacket {
        PublishPacket {
            message: PublishMessage::new(topic, Bytes::new()),
            meta: PublishPacketMeta {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
            },
            wire_topic_alias: alias.and_then(NonZeroU16::new),
        }
    }

    #[test]
    fn inbound_binds_then_resolves() {
        let mut aliases = InboundTopicAliases::new(4);
        let mut bind = packet("a/b", Some(2));
        aliases.resolve(&mut bind).expect("bind");
        assert_eq!(bind.wire_topic_alias, None);
        let mut reuse = packet("", Some(2));
        aliases.resolve(&mut reuse).expect("reuse");
        assert_eq!(&*reuse.message.topic, "a/b");
        // Rebinding the same alias to another topic replaces the mapping.
        aliases.resolve(&mut packet("c/d", Some(2))).expect("rebind");
        let mut again = packet("", Some(2));
        aliases.resolve(&mut again).expect("reuse rebound");
        assert_eq!(&*again.message.topic, "c/d");
    }

    #[test]
    fn inbound_rejects_unbound_and_out_of_range_aliases() {
        let mut aliases = InboundTopicAliases::new(2);
        assert!(aliases.resolve(&mut packet("", Some(1))).is_err(), "unbound");
        assert!(aliases.resolve(&mut packet("x", Some(3))).is_err(), "above maximum");
        assert!(
            InboundTopicAliases::new(0).resolve(&mut packet("x", Some(1))).is_err(),
            "aliases not accepted"
        );
        assert!(aliases.resolve(&mut packet("x", None)).is_ok(), "no alias is fine");
    }

    #[test]
    fn outbound_assigns_reuses_and_evicts_least_recently_used() {
        let mut aliases = OutboundTopicAliases::new(2);
        let (a, b, c) = (ByteString::from("a"), ByteString::from("b"), ByteString::from("c"));
        let nz = |n| NonZeroU16::new(n).expect("nz");

        let first = aliases.choose(&a).expect("enabled");
        assert_eq!(first, OutboundTopicAlias::Assign(nz(1)));
        aliases.commit(&a, first);
        assert_eq!(aliases.choose(&a), Some(OutboundTopicAlias::Reuse(nz(1))));

        let second = aliases.choose(&b).expect("enabled");
        assert_eq!(second, OutboundTopicAlias::Assign(nz(2)));
        aliases.commit(&b, second);
        // Touch `a` so `b` is the least recently used.
        aliases.commit(&a, OutboundTopicAlias::Reuse(nz(1)));

        let third = aliases.choose(&c).expect("enabled");
        assert_eq!(third, OutboundTopicAlias::Assign(nz(2)), "rebinds b's alias");
        aliases.commit(&c, third);
        assert_eq!(aliases.choose(&c), Some(OutboundTopicAlias::Reuse(nz(2))));
        assert_eq!(
            aliases.choose(&b),
            Some(OutboundTopicAlias::Assign(nz(1))),
            "b lost its alias"
        );
    }

    #[test]
    fn outbound_choose_without_commit_binds_nothing() {
        let aliases = OutboundTopicAliases::new(1);
        let topic = ByteString::from("t");
        assert!(matches!(aliases.choose(&topic), Some(OutboundTopicAlias::Assign(_))));
        assert!(
            matches!(aliases.choose(&topic), Some(OutboundTopicAlias::Assign(_))),
            "still unbound"
        );
        assert_eq!(OutboundTopicAliases::new(0).choose(&topic), None);
    }
}
