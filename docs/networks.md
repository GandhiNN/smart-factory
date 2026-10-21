# OPCUA Networking Concepts

## First principles: bind address vs. advertised address

Two separate concepts that people conflate:

- **Bind host** - the local interface the socket listens on. `0.0.0.0` = all interfaces (reachable from outside). This is purely local to the host machine.
- **Advertised host** - the hostname/IP the server puts in its discovery response and endpoint URLs. This must be an address the _remote client_ can resolve and reach. After the initial connect, clients reconnect to this advertised URL. So if it says `localhost`, a remote client fails.

The easiest setup is to _bind_ `0.0.0.0` but _advertise_ `your-vm.example.com` (or the host machine's public IP). The port is shared.

