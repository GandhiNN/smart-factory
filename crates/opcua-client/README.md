# What does an opcua-client do?

A client does four things in sequence:

1. **Connect & create a session** - open a secure channel to the server endpoint, authenticate (anonymous here), get a session.
2. **Create a subscription** - tell the server "I want to be notified of changes," with a publishing interval (how often the server batches notifications).
3. **Create monitored items** - within that subscription, register the specific nodes we care about, each with a sampling interval and optional deadband filter. The server samples each node and, when it changes, queues a notification.
4. **Receive notifications** - the server pushes `DataValue`s (value + status + timestamps) as they change. This is the inverse of the server's `set_values`

The key conceptual pairing: the server's `set_values` -> the client's monitored-item notification. That's the OPC-UA client/server subscription model end-to-end.
