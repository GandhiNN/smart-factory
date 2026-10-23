# Smart-Factory: The Concepts, Intuitively

The whole journey so far, explained as ideas rather than syntax. Think of it as
building a nervous system for a factory: sensors feel things, nerves carry
signals, and the brain needs to trust what it's hearing.

---

## The big picture: a chain from metal to insight

A physical fact — "this heater is at 162 degC" — travels from a sensor on the
factory floor all the way to an analytics system that can say "Line 1's quality
is drifting." That journey crosses several worlds, each with its own language,
and most of the hard parts are at the **borders** between those worlds.

Layers, bottom to top:

```
physical process -> OPC-UA -> MQTT -> Kafka -> analytics
                 (plant-floor) (messaging) (backbone)
```

The first two borders are built and proven.

---

## Lesson 1: Keep the "what" separate from the "how it travels"

`sfp-sim-core` knows about heaters, moisture, weights, and faults — but knows
*nothing* about OPC-UA, MQTT, or networking. That separation is the backbone of
the design.

Intuition: the *meaning* of a temperature reading doesn't change whether you
send it by OPC-UA, MQTT, carrier pigeon, or print it to a screen. So meaning
lives in one place (the core), and each transport layer is a translator bolted
on the side. A thing that depends on nothing can be reused by everything; a
thing everything depends on must never reach back down into them.

---

## Lesson 2: A signal is a little physics simulation

Each `Signal` doesn't just hold a number — it *behaves*. It drifts toward its
setpoint (first-order lag: "move a fraction of the remaining distance each
tick"), jitters with noise, and can break. The noise scales to each signal's
natural range (the "EU span" = `eu_high - eu_low`), so a thermocouple and a
weigh scale both get *proportional* realism without hand-tuning.

Intuition: real sensors are lazy and noisy. They ease toward the target and
wobble. Modeling that makes the downstream data worth analyzing — flat, perfect
numbers would teach the analytics nothing.

---

## Lesson 3: Quality is a first-class citizen, carried end to end

The most important thread in the project. Every reading carries a **quality**:
Good, Uncertain, or Bad. A dead sensor doesn't vanish — it keeps reporting, but
flagged "don't trust me." That quality rides from the sim, through OPC-UA's
`StatusCode`, into the MQTT payload, and out the other end.

Intuition: the dangerous failure isn't a sensor that stops — it's a sensor that
keeps lying while everyone believes it. Carrying quality lets a downstream
system tell "162 degC, trustworthy" from "162 degC, but the probe is flaky."
Proven: a fault injected in the sim showed up as `Uncertain` at the MQTT
subscriber, untouched across three protocol boundaries.

Translation happens in exactly *one* place (`mapping.rs`: domain `Quality` ->
OPC-UA `StatusCode`). One translator per border, not scattered conversions.

---

## Lesson 4: OPC-UA is a structured, subscribable address space

The OPC-UA server exposes the line as a *tree of nodes* — a folder for the line,
a node per signal — each with a value and a status. Clients don't poll; they
**subscribe** and get pushed changes. The server runs a timer (the "scan cycle,"
like a PLC) that advances the sim and pushes new values.

Intuition: OPC-UA is a shared, live spreadsheet the plant floor publishes and
anyone authorized can watch. You don't ask "what's the temperature now? now?
now?" — you say "tell me whenever it changes," and it does.

---

## Lesson 5: The brutal debugging lesson — don't guess, instrument

The subscription silently delivered nothing. Root cause: node ownership in the
OPC-UA library is decided purely by *namespace URI*, and the application URI
accidentally collided with the custom namespace URI — so a built-in diagnostics
manager hijacked the nodes and quietly rejected the subscription.

The real lesson: the namespace index was guessed twice and wrong twice. The fix
came from turning on the server's debug logs and *seeing* the request routed to
the wrong handler. When something is invisible, make it visible — add logging,
read the actual routing — don't keep poking at guesses. "Make the invisible
visible" beats "try another tweak."

---

## Lesson 6: Resolve at runtime, don't hardcode what the system can tell you

Two fragile hardcodes were replaced with runtime discovery:

- **Namespace index:** instead of hardcoding "my namespace is index 2," the
  client *asks* the server (read the NamespaceArray, find my URI). If the
  server's layout shifts, the client still works.
- **Node discovery (browsing):** instead of a hardcoded list of nodes to
  monitor, the bridge *browses* the address space — "what variables live under
  Line1?" — and subscribes to whatever it finds.

Intuition: hardcoding is a promise that the world will never change. It always
does. Asking the system to describe itself means the client adapts
automatically. Proven: the bridge discovered all 6 nodes without being told the
number.

---

## Lesson 7: Config-driven beats recompiling

The line definition moved out of Rust code and into `config/line1.toml`. Add a
sensor by editing the config — no code change, no rebuild. And without polluting
the pure core: serde/TOML sit behind a feature flag, so code that doesn't need
config never pays for it.

Intuition: the *shape* of a factory line is data, not logic. Data belongs in
data files. The payoff with browsing: edit the TOML -> the server builds a new
address space -> the bridge discovers the new node -> it flows to MQTT, with
zero code touched.

---

## Lesson 8: The bridge is a translator, not a dependency

The MQTT bridge does the same OPC-UA subscription as the diagnostic client, but
instead of *printing* it *publishes*. Neither depends on the other; they share a
pattern. The bridge is the literal OT->IT handoff — where plant-floor language
becomes IT messaging language.

Intuition: a translator at a border doesn't own either country. It faithfully
converts what crosses. Keeping it its own program mirrors how a real edge
gateway is its own deployable box.

---

## Lesson 9: The borders are where the bugs live

Almost every real problem was at a *seam*, not inside a layer:

- `localhost` resolving to IPv6 `::1` while the server bound IPv4 `0.0.0.0` — a
  networking seam.
- The `aws-lc` linker failure from a TLS backend that wasn't needed — a
  build/toolchain seam (fixed by disabling a default feature).
- The `s=` prefix leaking into MQTT topics because stringifying a NodeId
  includes its type tag — an encoding seam.
- The `, Overflow` status bit — an OPC-UA subscription-queue detail bleeding
  into otherwise clean data.

Intuition: each layer is usually fine on its own. It's where two systems *meet*
— different address families, toolchains, string encodings, status semantics —
that assumptions clash. Debugging a pipeline: suspect the joints first.

---

## Lesson 10: Prove it, don't assume it

"It compiled" was never treated as "it works." Real JSON was watched arriving at
a subscriber, the fault quality was seen propagating, the discovered node count
was confirmed. And the *actual library source* was read to confirm APIs instead
of guessing — because guessing caused Lesson 5.

Intuition: a green build means the grammar is correct, not that the story is
true. The only proof is seeing the real data do the real thing end to end.

---

## Lesson 11 (concept): make absence speak — Last Will & birth/death

The deepest idea yet, from Sparkplug B: **arrange for the broker to speak for you
when you can't.** Pre-register a "death notice" (MQTT Last Will) at connect time,
so if the gateway crashes, the broker announces its death on its behalf after it
stops hearing the heartbeat. On a clean exit, the gateway announces it itself
(a clean disconnect cancels the armed Will). Combined with *retained* messages
(the broker keeps the last status as a sticky note for anyone who joins later), a
downstream system can always tell live data from a dead gateway's stale ghost.

Key sub-points:
- Death is published in **both** paths — by you on clean shutdown, by the broker
  on a crash — because you can't predict which death you'll get.
- A crash isn't detected instantly: the broker fires the Will after ~1.5x the
  keep-alive interval of silence (no real traffic and no PINGREQ heartbeat).
- **Retained** is the load-bearing word: subscription alone isn't enough, because
  normal messages only reach whoever was listening at publish time. Retention is
  what replays the last birth/death to a late-joining consumer.

Intuition: silence is ambiguous — is it calm or catastrophe? This makes absence
*speak*. That's the whole reason it exists.

---

## The one-sentence compression

Put meaning in one place, translate it carefully at each border, let the system
describe itself instead of hardcoding, and never trust a reading — or a build —
until it's proven and its trustworthiness travels with it.
