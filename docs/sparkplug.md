# The problem Sparkplug solves

Plain MQTT is just a dumb pipe: topics are arbitrary strings, payloads are arbitrary bytes.
That's flexible but it leaves three hard problems unsolved in a real plant:

1. **State / staleness.** MQTT is fire-and-forget. If your bridge crashes, the last values it published sit forever on the broker as "retained" messages. A consumer connecting later can't tell if that `163.0 degC` isl ive or three hours stale from a dead gateway. There's no built-in "this device is offline" signal.

2. **Discovery.** A new consumer joining has no idea what devices or metrics exist. It has to already know the topic strings. Your bridge solved discovery on the _OPC-UA_ side (browsing), but on the MQTT side, a Kafka consumer still has to be told "subscribe to `smart-factory/#` and parse these fields."

3. **Payload semantics + efficiency.** Everyone invents their own JSON shape. No standard for datatypes, timestamps, quality. JSON is also verbose - sending the full topic string + field names every 500ms per metric adds up at plant scale (thousands of tags).

Sparkplug B is an Eclipse specification that standardizes all three on top of plain MQTT. It doesn't replace MQTT - it's a set of rules for topcis, payloads, and session management.

## The three pillars

1. **A fixed topic namespace**. Every Sparkplug topic follows:
spBv1.0/{group_id}/{message_type}/{edge_node_id}/{device_id}
- `spBv1.0` - the Sparkplug B version literal.
- `group_id` - logical grouping (e.g. `smart-factory`).
- `message_type` - one of a fixed set (see below).
- `edge_node_id` - the gateway (e.g. `Line1` or your bridge).
- `device_id` - optional, a device behind the gateway.

Compare the following:
`spBv1.0/smart-factory/NDATA/Line1` vs. `smart-factory/Line1/HeaterZone1/Temperature`

2. **Birth/death certificates (the state solution).** This is the heart of Sparkplug. Message types:
- **NBIRTH** - "Node BIRTH": sent once when the edge node (gateway) connects. It's a full snapshot: every metric this node will report, with its datatype, current value, and a freshly reset sequence number. This is the discovery payload - a consumer reads one NBIRTH and knows the entire tag list.
- **NDATA** - "Node DATA": sent on change after birth. Only the metrics that changed (report-by-exception), not the full set. Efficient.
- **NDEATH** - "Node DEATH": the offline signal. Here's the clever part - the gateway registers this as an MQTT **Last Will and Testament** or **(LWT)** when it connects. The broker itself publishes the NDEATH automatically if the gateway's connection drops. So consumers get a definitive "Line1 is offline" even on a hard crash. This is what plain MQTT can't do.
- **DBIRTH / DDATA / DDEATH** - the same three for devices behind the node (one level down).
- **STATE** - for the consuming applications (SCADA/host) to announce their own online/offline.

3. **Binary payloads with sequence numbers.** Payloads are Google Protocol Buffers (protobuf), not JSON. A Sparkplug payload carries a timestamp, a `seq` number (0-255, incrementing each message), and a list of metrics. Each metric has a name, datatype, value, timestamp, and optionally a quality/properties set.

Two efficiency tricks ride on this:
- **Aliases.** The NBIRTH maps each long metric name (`Line1/HeaterZone1/Temperature`) to a small integer alias. Subsequent NDATA messages sned just the alias, not the string. Big bandwidth saving at scale.
- **The `seq` number** lets a consumer detect a missed message (gap in sequence) -> it knows its view is stale and can request a rebirth. 

## How this maps into this projcet

The `mqtt-bridge` is exactly a Sparkplug **Edge Node.**

The upgrade would be:

```bash
| **mqtt-bridge** | **Sparkplug B** |
|-----------------|-----------------|
| Topic `smart-factory/Line1/HeaterZone1/Temperature` | `spBv1.0/smart-factory/NDATA/Line1`, metric name inside payload |
| One message per signal | One payload carrying all changed metrics |
| JSON `{value, quality, status, ts}` | protobuf metric with typed value, timestamp, quality |
| No birth - consumer must know tags | NBIRTH on connect lists all 6 metrics + datatypes |
| Crash leaves stale retained values | NDEATH via MQTT LWT -> consumers know Line1 died |
| `quality: "Good"` string | Sparkplug metrics properties / quality code |
```

The OPC-UA side browsing actually makes NBIRTH _natural_: we already discover the full metric list at startup - that discovered list **is** the NBIRTH payload.

## The cost

Sparkplug is not free to adopt:

- **Protobuf dependency + schema.** The payload type is a generated protobuf struct. It's less transparent than JSON.
- **Sequence/state management.** We must track the `seq` counter, send NBIRTH before any NDATA, register the NDEATH as LWT at connect time, and handle rebirth request. More state machine that our current stateless publish.
- **Debugging opacity.** The MQTT client (e.g. `mosquitto_sub`) will show binary garbage instead of readable JSON. We will need a Sparkplug-decoding tool (or write a small decoder) to inspect traffic.

