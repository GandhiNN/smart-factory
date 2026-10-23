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

### How this maps into this projcet

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

### The cost

Sparkplug is not free to adopt:

- **Protobuf dependency + schema.** The payload type is a generated protobuf struct. It's less transparent than JSON.
- **Sequence/state management.** We must track the `seq` counter, send NBIRTH before any NDATA, register the NDEATH as LWT at connect time, and handle rebirth request. More state machine that our current stateless publish.
- **Debugging opacity.** The MQTT client (e.g. `mosquitto_sub`) will show binary garbage instead of readable JSON. We will need a Sparkplug-decoding tool (or write a small decoder) to inspect traffic.

## How does a consumer know whether the data it's seeing is live or from a dead gateway?

The core problem with MQTT: **It is stateless and connectionless (to consumers)**

When the (OPCUA-MQTT) bridge publishes `{value: 162.0}`, the broker fans it out to subscribers and forgets it. Now consider three failure modes:

1. **A consumer connects late.** It missed everything published so far. It sees nothing until the next value change - could be seconds, could be minutes for a slow signal.
2. **The bridge crashes.** It stops publishing. But a consumer that already received `162.0` has no idea the gateway died - it just sees _no new messages_, which is indistinguishable from "the value genuinely has not changed."
3. **Network partition.** Same as a crash from the consumer's point of view.

Plain MQTT gives you data but no **liveness** => In all three, the consumer can't answer "is this data trustworthy right now?"

## MQTT has two primitives that fix this

Sparkplug combines two features that already exist in MQTT:

### Primitive 1: Retained messages

When we publish with the `retain` flag set, the broker **keeps the last such message per topic** and immediately delivers it to any new subscriber. This solves failure mode 1: a late consumer gets the current state instantly on connect, instead of waiting for the next change.

So a **BIRTH** message - published retained - means "here's my full current state; anyone connecting gets it immediately."

### Primitive 2: Last Will and Testament (LWT)

This is the key idea, and it's beautifully simple. When a client _connects_ to the broker, it can hand the broker a **will**: "if i disconnect ungracefully (crash, network drop, no keepalive), publish _this_ message on _this_ topic on my behalf."

The broker holds that will. If the client disconnects cleanly, the will is discarded. If the connection dies unexpectedly, **the broker itself publishes the will.**

That's the magic: the death notification comes from the _broker_, not the dead process. A crashed bridge obviously can't announce its own death - but the broker, which noticed the TCP connection drop, can. This solves failure modes 2 and 3.

So a **DEATH** message - registered as the LWT at connect time - means "if you receive this, I'm offline; distrust my last values."

#### The lifecycle, step-by-step

Here's the full state machine your (OPCUA) bridge acting as an edge node called `Line1`:

1. **At connect - set the Will, before the connection opens.** WHen the bridge opens its MQTT connection, it includes in the CONNECT packet:
- a Will **topic** (where the death notice goes),
- a Will **payload** (the death notice content),
- Will **retained = true** and usually QoS 1.

The broker stores this but does _not_ publish it yet. It's armed, waiting.

2. **Right after connecting - publish the BIRTH.** The bridge's first act is to publish a **birth** message: "Line1 is online, here is my current state / metrics list." Retained = true, so any consumer connecting _later_ immediately gets the last birth and knows the node is alive and what it reports.

3. **While running - publish DATA.** Normal value changes, exactly what your bridge does now.

4. **On clean shutdown (Ctrl-C) - publish DEATH yourself, then disconnect.** If you shut down gracefully, you send the death message yourself and disconnect cleanly. A clean MQTT disconnect tells the broker "don't fire the Will" - so you must publish death manually in this path.

5. **On a crash / network drop - the broker fires the Will.** If the bridge dies without a clean disconnect, the broker eventually notices (via the keep-alive timeout - no PINGREQ within ~1.5x keep-alive) and publishes the stored Will message to the death topic on your behalf. Consumers see "Line1 is offline" without the bridge doing anything. 

So death gets published in **both** paths - by you on clean exit, by the broker on a dirty exit. That's the guarantee: a consumer _always_ learns the node went down.

#### Why retained matters here

MQTT "retained" means the broker keeps the last message on a topic and delivers it instantly to any new subscriber. For birth/death this is essential:

- **Birth retained**: a Kafka consumer that starts up 10 minutes after the bridge still immediately learns "Line1 is online, here are its metrics."
- **Death retained**: if the bridge is currently down, a new consumer immediately learns "Line1 is offline" rather than hearing silence and guessing.

The birth and death are published to the **same topic** (in Sparkplug, `NBIRTH/NDEATH` are different message types, but conceptually the "is this node alive" state lives on a status topic). Each new publish overwrites the retained one - so the retained message on that topic is always the _latest_ truth: alive after a birth, dead after a death.

#### The ordering rule that trips people up

The Will must be set **as part of the connect**, not after. You cannot connect, then decide to add a Will - the LWT is a field in the CONNECT packet itself. In `rumqttc` this means configuring it on `MqttOptions` _before_ `AsyncClient::new`. Get the order wrong and the death notice silently never fires.
This is the one subtle correctness point in the whole feature.

#### A concrete status-topic design for your bridge (plain JSON, pre-protobuf)

We don't need the full `spBv1.0/...` structure yet. A clean minimal version:

- **Status topic**: `smart-factory/Line1/status`
- **Birth payload** (published by bridge right after connect, retained):

```json
{"online": true, "ts": "2026-...", "metrics": ["Line1/HeaterZone1/Temperature", ...]}
``` 

The `metrics` list comes straight from our OPC-UA browse discovery -> this is the proto-NBIRTH.

- **Death payload** (set as Will at connect, and re-published on clean shutdown, retained):

```json
{"online": false}
```

A consumer subscribes to `smart-factory/Line1/status` and always knows the node's liveness and tag list, independent of the `smart-factory/Line1/<signal>` data stream.

Notice the dependency: The birth needs the discovered metric list, so birth must happen _after_ OPC-UA browse, but the Will must be set _before_ MQTT connect. So the metric list is not available when you set the Will - which is fine, because the Will (death) payload does not need the metric list, only `{"online": false}`

##### 1. Why death needs two code paths (it feels redundant?)

The two deaths are mutually exclusive, covering two different exit modes:

- **Clean exit (Ctrl-C)** - we call `disconnect()`. MQTT treats a clean disconnect as "this client left on purpose" and **discards the armed Will without firing it.** So if we _only_ relied on the Will, a graceful shutdown would never flip the status to offline. That's why we must publish death ourself in this path.
- **Dirty exit (crash, cable pull, process killed):** We never get to run any code. Only the broker can speak - and it fires the armed Will.

Summary: exactly one death message gets published per shutdown. Which mechanism sends it depends on _how_ you died. You cover both paths because you can't predict which death you'll get.

##### 2. How the broker actually _notices_ a dirty death (the timing)

MQTT has a **keep-alive** interval (configurable). The rule: the client must send _something_ at least once per keep-alive, and if it has no real traffic, it sends a tiny `PINGREQ` heartbeat. The broker's rule: if it hears nothing from a client for **1.5x keep-alive**, it declares the connection dead and fires the Will.

Consequence: death is **not instant** on a crash. With a 5s keep-alive, a consumer might not see "offline" for up to ~7.5s. That lag is inherent. We can tune it by lowering the keep-alive (faster detection, more heartbeat traffic). On a _clean_ disconnect there's no lag, because we published death immediately ourself.

##### 3. Retained = the broker's "last known state" sticky note

Normal MQTT messages are delivered only to whoever is subscribed _at that instant_; a subscriber joining one second later never sees them. **Retained** flips that: the broker keeps the last retained message per topic and hands it to every new subscriber immediately on subscribe.

This is right for a status topic: liveness is a _state_, not an _event_. A consumer joining at any time needs to know "is Line1 up right now?", not "did I happen to be listening at the moment it came up?" Birth and death are both retained and published to the same status topic, so the retained message there is always the latest truth. Birth overwrites death's retained message and vice versa.

Contrast with the per-signal data topics: those we'd typically _not_ retain (or retain only the latest reading), because a stale temperature from an hour ago is misleading - which is the very staleness problem Sparkplug's birth/death exists to flag.

##### 4. Who consumes this, and what they do with it?

The audience is the downstream IT consumer (e.g. Kafka connector, analytics host). Concretely a consumer:

- Subscribes to `smart-factory/Line1/status` and `smart-factory/Line1/#` (the data).
- On connect, the retained status tells it immediately whether `Line1` is alive and (from the birth payload) what metrics to expect - **discovery without hardcoding**, the MQTT-side mirror of our OPC-UA browse.
- If it later sees the status flip to `{"online": false}`, it knows every value under `Line1/` is now stale and can grey it out / alarm / stop trusting it
- Instead of sliently showing hour-old data as if live -> the entire _business reason_ why this feature exists: downstream systems can distinguish "live data" from "a dead gateway's ghost" which a plain pub/sub cannot

