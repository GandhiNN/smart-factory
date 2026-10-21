# Smart, Smoke-Free Factory

## 1. Canonical architecture

This often framed in terms of the ISA-95 / Purdue model that industrial facilities use:

```bash
LEVEL 0-1: Physical process + sensors/actuators
Thermocouples, flow meters, load cells, vision cameras, servo drives
    |    (4-20mA, digital I/O, Profinet, EtherCAT, Modbus)
    ▼
LEVEL 2: PLCs / controllers (Siemens S7, Rockwell, Beckhoff)
Real-time control loops, interlocks, machine state machines
    |
    ▼ ◄── OPC-UA server usually lives here or at a gateway
LEVEL 3: SCADA / MES / Historian / Edge
OPC-UA ──► MQTT / Kafka bridge
    |
    ▼
LEVEL 4-5: IT / Cloud - Kafka → stream processing → data lake → BI/ML
```

The key transition point is OT → IT: deterministic, machine-centric protocols on the plant floor give way to pub/sub messaging and event streaming
for analytics. OPC-UA is the bridge standard at that boundary.

## 2. What OPC-UA Actually Is

Open Platform Communications - Unified Architecture, IEC 62541, is a platform-independent, service-oriented standard for industrial interoperability.
It replaced the old Windows/COM-based "classic OPC".

Why it matters:

- **Information model, not just data.** Unlike Modbus (which gives us register '40001 = 2347' with no meaning), OPC-UA exposes an **address space** - a graph of nodes with types, semantics, units, and relationships. A node is not "register 40001", it's `HAZmixer/HearterZone1/Temperature`, a `Double`, unit `C`, with min/max engineering ranges attached. This self-description is exactly what makes downstream analytics tractable.
- **Rich data per value.** Every reading is a `DataValue: { value, statusCode, sourceTimestamp, serverTimestamp }`. The `statusCode` (Good / Bad / Uncertain) is critical, analytics must distinguish "temperature is 0" for "sensor is dead."
- **Two communication patterns**:
    - **Client/Server** - request/response and subscriptions (client subscribes to nodes, server pushes changes via "MonitoredItems" with sampling + deadband filters).
    - **PubSub** - broker-based or broadcast; and critically, **OPC-UA PubSub can run directly over MQTT**, which is our bridge to the next layer.
- **Security built in** - X.509 certificates, signing, encryption, user authentication, auditing. Important in regulated manufacturing.
- **Companion specifications** - standardized models per industry/device (e.g., PackML for packaging machines, Euromap for injection molding, Weihenstephan for bottling). These give common vocabulary across vendors.

## 3. Sensors & PLCs in an SFP manufacturing facility

Smoke-Free Products (heated-tobacco consumables like HEETS/sticks, and the devices themselves) share process traits with pharma and precision electronics - tight tolerances, heat-sensitive material, and heavy traceability/regulatory requirements. Typical instrumentation:

### Process/material side (consumable manufacturing):
- **Temperature** - thermocouples/RTDs on drying, casing, and heating zones. Heated-tobacco substrate must not combust, so thermal control is central.
- **Humidity/moisture** - the tobacco cast-leaf / reconstituted sheet has tight moisture-content windows; NIR moisture sensors and hygrometers.
- **Flow & dosing** - mass-flow meters and load cells for glycerin/humectant casing, flavor dosing.
- **Weight/density** - checkweighers, load cells per stick/pack.
- **Vision & dimensional** - cameras and laser micrometers for rod diameter, segment placement, print/marking inspection.
- **Speed/position** - encoders, servo drives on maker and packer lines.

### Device assembly side (the electronic heating device):
- Pick-and-place and SMT telemetry, torque drivers, functional-test rigs, battery-test cells, leak/seal testers.

### Controllers
- **PLCs** (Siemens S7-1500, Rockwell ControlLogix, Beckhoff) run the real-time loops and safety interlocks. They usually don't talk analytics protocols directly.
- **OPC-UA server** is exposed either _embedded_ in the PLC (modern Siemens/Beckhoff do this natively) or via a **gateway/edge device** (Kepware, Ignition, Matrikon) that polls PLCs over their native fieldbus and republishes as OPC-UA.

So the flow at the plant is:

```bash
sensor -> PLC (control) -> OPC-UA server (contextualized exposure) -> everything upstream reads OPC-UA.
```

## 4. OPC-UA -> MQTT

Two distinct ways this is done (the distinction matters):

### (a) OPC-UA PubSub over MQTT (native)
The OPC-UA spec defines how to publish `DataSetMessages` to an MQTT broker, encoded as JSON or binary (UADP). The payloads still carry OPC-UA semantics. Good when both ends are OPC-UA-aware.

### (b) Bridge/gateway mapping (most common in practice).
An edge app subscribes to OPC-UA nodes (client/server subscription) and republishes each change to MQTT topics. This is where **Sparkplug B** usually enters -> an MQTT payload specification (from the Eclipse Tahu project) purpose-built for industrial data:
- Defines a topic namespace: `spBv1.0/{group}/{msg_type}/{edge_node}/{device}`
- Message types: `NBIRTH/DBIRTH` (birth certs that publish the full tag list + metadata once), `NDATA/DDATA` (subsequent changes only), `NDEATH/DDEATH` (via MQTT Last-Will for instant offline detection).
- Report-by-exception + stateful awareness - far more efficient and robust than naive "one topic per tag."

MQTT is used here because it's: lightweight, pub/sub, tolerant of unreliable plant networks, QoS levels, and retained/last-will semantics. It decouples produces from consumers.

## 5. MQTT -> Kafka

MQTT and Kafka solve different problems and are complementary, not competing:

```bash
 ┌────────────┬───────────────────────────────────────────────┬───────────────────────────────────────────────┐
  │            │ MQTT                                          │ Kafka                                         │
  ├────────────┼───────────────────────────────────────────────┼───────────────────────────────────────────────┤
  │ Model      │ Pub/sub messaging                             │ Distributed, durable event log                │
  ├────────────┼───────────────────────────────────────────────┼───────────────────────────────────────────────┤
  │ Strength   │ Many lightweight edge clients, flaky networks │ High-throughput, replayable, retained history │
  ├────────────┼───────────────────────────────────────────────┼───────────────────────────────────────────────┤
  │ Retention  │ Transient (mostly)                            │ Durable, long retention, re-consumable        │
  ├────────────┼───────────────────────────────────────────────┼───────────────────────────────────────────────┤
  │ Scale unit │ Devices/topics                                │ Partitions/consumer groups                    │
  └────────────┴───────────────────────────────────────────────┴───────────────────────────────────────────────┘
```

Typical integration: A **Kafka Connect MQTT source connector** (or a tool like HiveMQ's Kafka extension) consumes MQTT topics and writes them into Kafka topics. Kafka then becomes the backbone where multiple independent consumers (analytics, historian, ML feature pipeline, monitoring) each read the same stream at their own pace, and we can **replay** history.

Common topic design: one Kafka topic per logical stream (e.g., `sfp.line1.maker.telemetry`), partitioned by machine/line for ordering and parallelism.

## 6. Analytics at the end

Once data is in Kafka, we fan out to:

- **Stream processing** - Kafka Streams / Flink/ ksqlDB for windowed aggregations, SPC (statistical process control) rules, OEE (availability x performance x quality) computation, anomaly detection in real time.
- **Time-series storage** - InfluxDB / TimescaleDB / historians for dashboards (Grafana) and trend analysis.
- **Data lake / warehouse** - S3/Delta/Snowflake/BigQuery for batch ML, quality correlation (e.g., link moisture-sensor drift to downstream reject rates), yield analysis, predictive maintenance.
- **ML feedback** - models can push setpoint recommendations back toward the edge (closing the loop), though in regulated SFP manufacturing that path is usually advisory, not autonomous.

The payoff of OPC-UA's rich model shows up here: because every tag arrived with type, unit, and quality, analytics does not need a brittle hand-maintained spreadsheet mapping "register 40001 = heater temp"

