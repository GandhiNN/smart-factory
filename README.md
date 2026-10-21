# Smart-Factory

## The one big idea

**Separate the process from how we _observe_ it**

A real plant floor exists whether or not anyone is watching. Tobacco sheet moves through heater zones, sticks get weighed, the machine runs or trips -> all of that is physical reality. _Then_, separately, we bolt on ways to observe it: an OPC-UA server, a Modbus gateway, a dashboard.

We mirrored that split in code:

- `sfp-sim-core` = the process. Knows about temperatures, moisture, faults, quality. Knows _nothing_ about OPC-UA.
- `opcua-server` = one way of observing it. Knows the OPC-UA protocol. Reads from the core and republishes.

The process never depends on the protocol.

**A sensor reading is not a number - it's a number _plus_ trust**

The naive mental model is "a sensor gives you 162.3." The real model is "a sensor gives you 162.3, **and** a claim about whether you should believe it."

Hence, in the core module, the `Quality` (Good / Uncertain / Bad) is a first-class type. A dead thermocouple still emits a number - the danger is trusting it. Every downstream decision (control, analytics, alarms) hinges on distinguishing "temperature is genuinely 0°C" from "sensor is dead and happens to read 0." This condition is modeled by having a `Bad` sensor **stop tracking** the real process - its value goes stale while its quality tells the truth.

## Setpoint vs. value: _wanting_ vs. _being_

- **Setpoint** = where the process is _trying_ to be (162°C)
- **Value** = where it _actually_ is right now (wobbling around 162)

The gap between them is driven to zero by control. 

## A machine is **analog signals + a discrete state + a counter**

The `MakerLine` composes:
- **Analog signals** (the sensors) - continuous, noisy, can degrade in quality.
- **A discrete state** (Running / Stopped / Faulted) - categorical, not noisy. This is the backbone ov availability/OEE analytics = _was the line actually producing?_
- **A throughput counter** - only grows while Running.

## OPC-UA is a self-describing tree of live values
<TBC>

## The server's rhythm: scan, then publish
<TBC>