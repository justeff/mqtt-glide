//! Packet-identifier allocation with separate publish vs subscribe/unsubscribe pots.
//!
//! Outbound PUBLISH waiters and SUBSCRIBE/UNSUBSCRIBE waiters use
//! **separate inflight pots** (distinct waiter maps). MQTT still requires every
//! outstanding Packet Identifier to be unique across all packet types
//! ([MQTT-2.2.1-3]), so allocation draws from one shared unused-ID space while
//! recording ownership in the matching pot.
//!
//! Publish IDs are allocated in `1..=publish_capacity` via a dense [`Vec`] slot table
//! (ID = index). Subscribe/unsubscribe may use any free ID; IDs with value ≥
//! `slots.len()` live in a small [`HashSet`]. Growing the table migrates any
//! overlapping `subscribe_high` entries into slots so that invariant stays exact.

use std::collections::HashSet;
use std::num::NonZeroU16;

use crate::error::AppError;

/// Occupancy of one packet-id slot in `1..=publish_capacity` (and retained larger tables).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Slot {
    #[default]
    Free,
    Publish,
    Subscribe,
}

/// Separate publish and subscribe/unsubscribe inflight pots over a shared ID space.
///
/// Publish allocation uses a dense slot `Vec` sized to the Receive Maximum window.
#[derive(Debug)]
pub(crate) struct InflightPots {
    /// Index = packet id; index 0 unused. Length is at least `publish_capacity + 1`.
    slots: Vec<Slot>,
    /// Subscribe/unsubscribe IDs with value ≥ `slots.len()` (outside the dense table).
    subscribe_high: HashSet<NonZeroU16>,
    /// Outstanding publish IDs (soft-capped by [`Self::publish_capacity`]).
    publish_count: u16,
    /// Next publish candidate in `1..=publish_capacity`.
    next_publish: u16,
    /// Next subscribe/unsubscribe candidate in `1..=u16::MAX`.
    next_control: u16,
    /// Soft cap on concurrent publish IDs (CONNACK `server_receive_maximum` / CONNECT window).
    publish_capacity: u16,
}

impl InflightPots {
    /// Creates empty pots with a publish capacity (typically `server_receive_maximum`).
    pub(crate) fn new(publish_capacity: NonZeroU16) -> Self {
        let cap = publish_capacity.get();
        Self {
            slots: vec![Slot::Free; usize::from(cap) + 1],
            subscribe_high: HashSet::new(),
            publish_count: 0,
            next_publish: 1,
            next_control: 1,
            publish_capacity: cap,
        }
    }

    /// Allocates a Packet Identifier for an outbound QoS > 0 PUBLISH.
    ///
    /// IDs are chosen from `1..=publish_capacity` only (dense slot table).
    pub(crate) fn alloc_publish(&mut self) -> Result<NonZeroU16, AppError> {
        if self.publish_count >= self.publish_capacity {
            return Err(AppError::Backpressure("publish inflight at capacity".into()));
        }
        self.ensure_slot_len(usize::from(self.publish_capacity) + 1);

        let mut candidate = Self::normalize_publish_id(self.next_publish, self.publish_capacity);
        for _ in 0..self.publish_capacity {
            let index = usize::from(candidate);
            // Invariant: IDs in `1..slots.len()` are never also in `subscribe_high`.
            if self.slots[index] == Slot::Free {
                self.slots[index] = Slot::Publish;
                self.publish_count += 1;
                self.next_publish = Self::next_publish_after(candidate, self.publish_capacity);
                return Ok(NonZeroU16::new(candidate).expect("publish window starts at 1"));
            }
            candidate = Self::next_publish_after(candidate, self.publish_capacity);
        }
        Err(AppError::Backpressure(
            "no free packet identifiers in publish window".into(),
        ))
    }

    /// Whether [`Self::alloc_publish`] can hand out another publish ID right now.
    #[inline]
    pub(crate) fn has_publish_capacity(&self) -> bool {
        self.publish_count < self.publish_capacity
    }

    /// Allocates a Packet Identifier for SUBSCRIBE or UNSUBSCRIBE.
    pub(crate) fn alloc_subscribe(&mut self) -> Result<NonZeroU16, AppError> {
        for _ in 0..u16::MAX {
            let candidate = if self.next_control == 0 { 1 } else { self.next_control };
            self.next_control = candidate.wrapping_add(1);

            let Some(id) = NonZeroU16::new(candidate) else {
                continue;
            };
            if self.in_use(id) {
                continue;
            }
            self.mark_subscribe(id);
            return Ok(id);
        }
        Err(AppError::Backpressure("no free packet identifiers".into()))
    }

    /// Releases a publish-owned Packet Identifier after PUBACK, encode failure, or session teardown.
    pub(crate) fn release_publish(&mut self, id: NonZeroU16) {
        let index = usize::from(id.get());
        if index < self.slots.len() && self.slots[index] == Slot::Publish {
            self.slots[index] = Slot::Free;
            self.publish_count = self.publish_count.saturating_sub(1);
        }
    }

    /// Resizes the publish inflight soft cap (CONNACK `server_receive_maximum`).
    ///
    /// Does not evict outstanding IDs; only affects subsequent [`Self::alloc_publish`] calls.
    /// Growing extends the slot table and migrates overlapping [`Self::subscribe_high`]
    /// entries into slots; shrinking leaves existing slots in place.
    pub(crate) fn set_publish_capacity(&mut self, publish_capacity: NonZeroU16) {
        let cap = publish_capacity.get();
        self.publish_capacity = cap;
        self.ensure_slot_len(usize::from(cap) + 1);
        if self.next_publish == 0 || self.next_publish > cap {
            self.next_publish = 1;
        }
    }

    /// Current publish soft-cap (for tests / observability).
    #[allow(dead_code)] // unit tests
    pub(crate) fn publish_capacity(&self) -> u16 {
        self.publish_capacity
    }

    /// Releases a subscribe-owned Packet Identifier after SUBACK/UNSUBACK, send failure, or session teardown.
    pub(crate) fn release_subscribe(&mut self, id: NonZeroU16) {
        let index = usize::from(id.get());
        if index < self.slots.len() && self.slots[index] == Slot::Subscribe {
            self.slots[index] = Slot::Free;
            return;
        }
        self.subscribe_high.remove(&id);
    }

    /// Returns whether `id` is currently held in the publish pot.
    #[allow(dead_code)] // unit tests
    pub(crate) fn publish_contains(&self, id: NonZeroU16) -> bool {
        let index = usize::from(id.get());
        index < self.slots.len() && self.slots[index] == Slot::Publish
    }

    /// Returns whether `id` is currently held in the subscribe pot.
    #[allow(dead_code)] // unit tests
    pub(crate) fn subscribe_contains(&self, id: NonZeroU16) -> bool {
        let index = usize::from(id.get());
        if index < self.slots.len() && self.slots[index] == Slot::Subscribe {
            return true;
        }
        self.subscribe_high.contains(&id)
    }

    fn in_use(&self, id: NonZeroU16) -> bool {
        let index = usize::from(id.get());
        if index < self.slots.len() && self.slots[index] != Slot::Free {
            return true;
        }
        self.subscribe_high.contains(&id)
    }

    fn mark_subscribe(&mut self, id: NonZeroU16) {
        let index = usize::from(id.get());
        if index < self.slots.len() {
            debug_assert_eq!(self.slots[index], Slot::Free);
            self.slots[index] = Slot::Subscribe;
        } else {
            self.subscribe_high.insert(id);
        }
    }

    fn ensure_slot_len(&mut self, len: usize) {
        if self.slots.len() >= len {
            return;
        }
        self.slots.resize(len, Slot::Free);
        self.migrate_subscribe_high_into_slots();
    }

    /// Moves `subscribe_high` IDs that now fall inside `slots` into the dense table.
    ///
    /// Keeps the invariant: every ID in `subscribe_high` has `id.get() as usize >= slots.len()`.
    fn migrate_subscribe_high_into_slots(&mut self) {
        if self.subscribe_high.is_empty() {
            return;
        }
        let limit = self.slots.len();
        let mut still_high = HashSet::new();
        for id in self.subscribe_high.drain() {
            let index = usize::from(id.get());
            if index < limit {
                debug_assert_eq!(self.slots[index], Slot::Free);
                self.slots[index] = Slot::Subscribe;
            } else {
                still_high.insert(id);
            }
        }
        self.subscribe_high = still_high;
    }

    const fn normalize_publish_id(id: u16, capacity: u16) -> u16 {
        if id == 0 || id > capacity { 1 } else { id }
    }

    const fn next_publish_after(id: u16, capacity: u16) -> u16 {
        if id >= capacity { 1 } else { id + 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn subscribe_packet_ids_do_not_collide_with_publish_inflight() {
        let mut pots = InflightPots::new(nonzero!(2u16));
        let publish_a = pots.alloc_publish().expect("publish a");
        let publish_b = pots.alloc_publish().expect("publish b");
        let subscribe = pots.alloc_subscribe().expect("subscribe must still allocate");

        assert_ne!(subscribe, publish_a);
        assert_ne!(subscribe, publish_b);
        assert!(pots.publish_contains(publish_a));
        assert!(pots.publish_contains(publish_b));
        assert!(!pots.publish_contains(subscribe));
        assert!(pots.subscribe_contains(subscribe));
        assert!(!pots.subscribe_contains(publish_a));
    }

    #[test]
    fn releasing_publish_id_does_not_clear_subscribe_waiter() {
        let mut pots = InflightPots::new(nonzero!(4u16));
        let publish = pots.alloc_publish().expect("publish");
        let subscribe = pots.alloc_subscribe().expect("subscribe");
        pots.release_publish(publish);
        assert!(!pots.publish_contains(publish));
        assert!(pots.subscribe_contains(subscribe));
    }

    #[test]
    fn publish_capacity_does_not_block_subscribe_allocation() {
        let mut pots = InflightPots::new(nonzero!(1u16));
        let _publish = pots.alloc_publish().expect("publish");
        assert!(matches!(pots.alloc_publish(), Err(AppError::Backpressure(_))));
        let subscribe = pots
            .alloc_subscribe()
            .expect("subscribe independent of publish capacity");
        assert!(pots.subscribe_contains(subscribe));
    }

    #[test]
    fn publish_packet_id_remains_allocated_until_explicit_release() {
        let mut pots = InflightPots::new(nonzero!(1u16));
        let id = pots.alloc_publish().expect("publish");
        assert!(pots.publish_contains(id));
        assert!(
            matches!(pots.alloc_publish(), Err(AppError::Backpressure(_))),
            "ID must stay held until definitive completion (PUBACK / encode error / session teardown)"
        );
        pots.release_publish(id);
        assert!(!pots.publish_contains(id));
        let _reused = pots
            .alloc_publish()
            .expect("capacity must free for a new alloc after definitive release");
    }

    #[test]
    fn set_publish_capacity_applies_connack_server_receive_maximum() {
        let mut pots = InflightPots::new(nonzero!(1024u16));
        assert_eq!(pots.publish_capacity(), 1024);
        pots.set_publish_capacity(nonzero!(1u16));
        assert_eq!(pots.publish_capacity(), 1);
        let _first = pots.alloc_publish().expect("first");
        assert!(
            matches!(pots.alloc_publish(), Err(AppError::Backpressure(_))),
            "after CONNACK reseed, outbound QoS1 inflight must respect server_receive_maximum"
        );
    }

    #[test]
    fn publish_ids_stay_within_receive_maximum_window() {
        let mut pots = InflightPots::new(nonzero!(4u16));
        for _ in 0..4 {
            let id = pots.alloc_publish().expect("publish");
            assert!(id.get() <= 4, "publish id {} outside 1..=4", id.get());
        }
        assert!(matches!(pots.alloc_publish(), Err(AppError::Backpressure(_))));
    }

    #[test]
    fn growing_capacity_extends_publish_window() {
        let mut pots = InflightPots::new(nonzero!(1u16));
        let first = pots.alloc_publish().expect("first");
        assert_eq!(first.get(), 1);
        pots.set_publish_capacity(nonzero!(2u16));
        let second = pots.alloc_publish().expect("second after grow");
        assert_eq!(second.get(), 2);
    }

    #[test]
    fn growing_capacity_migrates_subscribe_high_into_slots() {
        let mut pots = InflightPots::new(nonzero!(1u16));
        let _publish = pots.alloc_publish().expect("fill publish window");
        // With capacity 1, subscribe must take id ≥ 2 → `subscribe_high`.
        let subscribe = pots.alloc_subscribe().expect("subscribe outside window");
        assert!(subscribe.get() >= 2);
        assert!(pots.subscribe_high.contains(&subscribe));

        pots.set_publish_capacity(nonzero!(8u16));
        assert!(
            !pots.subscribe_high.contains(&subscribe),
            "grow must migrate overlapping subscribe ids out of subscribe_high"
        );
        assert!(pots.subscribe_contains(subscribe));
        assert_eq!(pots.slots[usize::from(subscribe.get())], Slot::Subscribe);

        // Migrated occupancy must still block publish reuse of that id.
        pots.release_publish(NonZeroU16::new(1).expect("nz"));
        let mut allocated = 0u16;
        loop {
            match pots.alloc_publish() {
                Ok(id) => {
                    assert_ne!(id, subscribe);
                    allocated += 1;
                }
                Err(AppError::Backpressure(_)) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert_eq!(
            allocated, 7,
            "8-slot window with one migrated subscribe leaves 7 publish ids"
        );
    }
}
