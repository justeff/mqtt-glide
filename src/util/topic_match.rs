//! Topic Filter matching (§4.7), for routing inbound messages by filter.

/// Whether `topic` matches the Topic Filter `filter` by the MQTT rules (§4.7).
///
/// `+` matches exactly one level (also an empty one: `a/+/c` matches `a//c`), `#` matches the
/// rest including the parent level (`a/#` matches `a`), and a filter starting with a wildcard
/// does not match a topic starting with `$` such as `$SYS/...` ([MQTT-4.7.2-1]). A shared
/// subscription filter (`$share/<group>/<filter>`) matches like its `<filter>` part.
///
/// An invalid filter (a wildcard that is not a whole level, `#` that is not the last level,
/// an empty filter) and a topic containing a wildcard match nothing.
///
/// For routing, [Subscription Identifiers](crate::MqttClient::subscribe_with_id) let the
/// broker say which subscription a message matched, without matching it again.
///
/// # Examples
///
/// ```
/// use mqtt_glide::topic_matches;
///
/// assert!(topic_matches("vehicle/+/result/#", "vehicle/WVW123/result/job/42"));
/// assert!(topic_matches("$share/workers/vehicle/+/status", "vehicle/WVW123/status"));
/// assert!(!topic_matches("#", "$SYS/broker/uptime"));
/// ```
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let filter = match filter.strip_prefix("$share/") {
        Some(rest) => match rest.split_once('/') {
            Some((group, inner)) if !group.is_empty() => inner,
            _ => return false,
        },
        None => filter,
    };
    if filter.is_empty() || topic.contains(['+', '#']) {
        return false;
    }
    if topic.starts_with('$') && filter.starts_with(['+', '#']) {
        return false;
    }
    let mut topic_levels = topic.split('/');
    let mut filter_levels = filter.split('/').peekable();
    while let Some(level) = filter_levels.next() {
        match level {
            "#" => return filter_levels.peek().is_none(),
            "+" => {
                if topic_levels.next().is_none() {
                    return false;
                }
            }
            exact => {
                if exact.contains(['+', '#']) || topic_levels.next() != Some(exact) {
                    return false;
                }
            }
        }
    }
    topic_levels.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn topic_matching_follows_the_wildcard_rules() {
        for (filter, topic, expected) in [
            ("a/b", "a/b", true),
            ("a/b", "a/c", false),
            ("a/b", "a/b/c", false),
            ("a/+", "a/b", true),
            ("a/+", "a/b/c", false),
            ("a/#", "a", true),
            ("a/#", "a/b/c", true),
            ("#", "a/b", true),
            ("+/+", "a/b", true),
            ("+", "a/b", false),
            ("a/+/c", "a//c", true),
            ("/+", "/a", true),
            // [MQTT-4.7.2-1]
            ("#", "$SYS/x", false),
            ("+/x", "$SYS/x", false),
            ("$SYS/#", "$SYS/x", true),
            // Shared subscriptions match like their filter part.
            ("$share/g/a/+", "a/b", true),
            ("$share/g/#", "$SYS/x", false),
            ("$share//a", "a", false),
            ("$share/g", "g", false),
            // Invalid filters and topics match nothing.
            ("", "", false),
            ("a/#/b", "a/x/b", false),
            ("a#", "a#", false),
            ("a+/b", "a+/b", false),
            ("a/+", "a/+", false),
        ] {
            assert_eq!(topic_matches(filter, topic), expected, "{filter:?} vs {topic:?}");
        }
    }
}
