//! Sparkplug B payload construction for the edge node.
//! 
//! This module owns the Sparkplug-specific encoding so `main.rs` stays focused
//! on the OPC-UA/MQTT plumbing. It builds the two session-defining messages:
//! 
//! - **NBIRTH**: the "contract/legend". Published once per MQTT session right
//! after connect. Declares every metric (name + datatype + alias + current value),
//! carries `seq = 0`, and includes the `bdSeq` metric.
//! - **NDEATH**: armed as the MQTT Last Will *before* connect.
//! Carries the SAME `bdSeq` as the birth, so when the broker publishes it on a crash the
//! consumer can match this death to that birth's session.
//! 
//! Three numbers make Sparkplug a *session* protocol:
//! - `bdSeq`: which session/life we are on. Fixed per connection; identical in
//! NBIRTH and NDEATH. (A session id.)
//! - `seq`: message counter within the session. 0 at birth, +1 each message,
//! wraps 255 -> 0. (Page numbers, so a gap is detectable.)
//! - `alias`: a small integer nickname for a metric, defined in NBIRTH and used
//! instead of the long name in NDATA. (A map legend.)

use std::collections::HashMap;

use sparkplug_rs::protobuf::Message;
use sparkplug_rs::sparkplug_b::{payload::Metric, Payload};

/// Sparkplug B datatype codes (from the spec; `datatype` is a plain uint32).
pub mod datatype {
    pub const INT32: u32 = 3;
    pub const INT64: u32 = 4;
    pub const UINT32: u32 = 7;
    pub const UINT64: u32 = 8;
    pub const DOUBLE: u32 = 10;
    pub const BOOLEAN: u32 = 11;
    pub const STRING: u32 = 12;
}

/// Metric name reserved by the spec for the birth/death sequence number.
const BDSEQ_METRIC_NAME: &str = "bdSeq";

/// A metric we will report: its Sparkplug name, assigned alias, datatype, and
/// current value (as a Sparkplug metric value).
#[derive(Debug, Clone)]
pub struct BirthMetric {
    pub name: String,
    pub alias: u64,
    pub datatype: u32,
    pub value: sparkplug_rs::sparkplug_b::payload::metric::Value,
}

/// Current wall-clock time in milliseconds since the Unix epoch as Sparkplug
/// payloads use a uint64 millisecond timestamp.
pub fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build a single Sparkplug `Metric`
fn make_metric(
    name: Option<&str>,
    alias: Option<u64>,
    datatype: u32,
    value: sparkplug_rs::sparkplug_b::payload::metric::Value,
) -> Metric {
    let mut m = Metric::new();
    if let Some(n) = name {
        m.name = Some(n.to_string());
    }
    m.alias = alias;
    m.datatype = Some(datatype);
    m.timestamp = Some(now_millis());
    m.value = Some(value);
    m
}

/// Build and encode the NDEATH payload for the Last Will.
/// 
/// Per spec the NDEATH contains only the `bdSeq` metric (same value as the
/// coming NBIRTH). It does NOT carry a `seq` (it is published by the broker,
/// outside our sequence). Returns protobuf-encoded bytes ready to arm as the
/// MQTT Will.
pub fn encode_ndeath(bd_seq: u64) -> Vec<u8> {
    use sparkplug_rs::sparkplug_b::payload::metric::Value;

    let mut payload = Payload::new();
    payload.timestamp = Some(now_millis());
    // NDEATH carries the bdSeq as a Long (uint64) metric.
    payload.metrics.push(make_metric(
        Some(BDSEQ_METRIC_NAME),
        None,
        datatype::UINT64,
        Value::LongValue(bd_seq),
    ));

    payload
        .write_to_bytes()
        .expect("NDEATH payload should always encode")
}

/// Build and encode the NBIRTH payload.
/// 
/// `seq` is 0 for a birth (spec rule). Includes the `bdSeq` metric (same value
/// armed in the Will) plus one metric per discovered node.
pub fn encode_nbirth(bd_seq: u64, metrics: &[BirthMetric]) -> Vec<u8> {
    use sparkplug_rs::sparkplug_b::payload::metric::Value;

    let mut payload = Payload::new();
    payload.timestamp = Some(now_millis());
    payload.seq = Some(0); // NBIRTH always resets the sequence to 0.

    // bdSeq metric first (convention): ties this birth to its matching death.
    payload.metrics.push(make_metric(
        Some(BDSEQ_METRIC_NAME),
        None,
        datatype::UINT64,
        Value::LongValue(bd_seq),
    ));

    // One metric per discovered node, each with its alias + current value.
    for bm in metrics {
        payload.metrics.push(make_metric(
            Some(&bm.name),
            Some(bm.alias),
            bm.datatype,
            bm.value.clone(),
        ));
    }

    payload
        .write_to_bytes()
        .expect("NBIRTH payload should always encode")
}

/// A monotonic sequence counter for NDATA messages (0..=255, wrapping).
/// NBIRTH used 0, so the first NDATA is 1.
#[derive(Debug)]
pub struct SeqCounter {
    next: u8,
}

impl SeqCounter {
    /// Create a counter whose first value is 1 (NBIRTH already consumed 0).
    pub fn after_birth() -> Self {
        Self { next: 1 }
    }

    /// Return the current value and advance (wrapping at 256).
    pub fn take(&mut self) -> u64 {
        let v = self.next;
        self.next = self.next.wrapping_add(1);
        v as u64
    }
}

/// The alias map: metric name -> alias, built during NBIRTH construction and
/// used by NDATA to refer to metrics by alias instead of name.
pub type AliasMap = HashMap<String, u64>;