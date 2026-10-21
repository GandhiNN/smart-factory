# What does an OPC-UA server actually do?

It holds an **address space** - a tree of nodes with stable identities (`NodeId`s), each carrying a value, a datatype, and a status.
Clients browse this tree, read nodes, and _subscribe_ to them (the server pushes changes). Our server will:

1. Build an address space that mirrors the line's structure: a folder per line, a variable per signal, plus machine state and throughput.
2. Run the core sim on a timer (the "PLC scan cycle").
3. Each tick, push updated values with **translated quality** into the address space so subscribed clients see them change.

**The key translation that happens here** => our domain `Quality` -> OPC-UA `StatusCode`. The core sim says `Quality::Bad`; the protocol layer turns into OPC-UA's `StatusCode::BadNoData` (or similar) attached to the `DataValue`. This is the boundary where "process truth" becomes "protocol representation." The core never knew what a `StatusCode` was (and shouldn't).

# The design

The server does three jobs:
1. **Translate** our domain `Quality` -> OPC-UA `StatusCode` (the protocol boundary).
2. **Build** and address space mirroring the line's structure.
3. **Run** the sim on a timer and push values into the address space.
