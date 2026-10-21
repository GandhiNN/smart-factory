//! OPC-UA client that subscribes to the simulated SFP maker line and prints
//! each value change to the terminal.
//! 
//! This is the *consumer* half of the OPC-UA client/server subscription model.
//! Where the server calls `set_values`, the client receives a data-change
//! notification for each monitored item. It mirrors exactly what the MQTT bridge
//! will do - except instead of printing, the bridge will publish.

use std::sync::Arc;
use std::time::Duration;

use log::warn;
use opcua::client::{
    ClientBuilder,
    DataChangeCallback,
    IdentityToken,
    MonitoredItem,
    Session
};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    DataValue,
    MessageSecurityMode,
    MonitoredItemCreateRequest,
    NodeId,
    StatusCode,
    TimestampsToReturn,
    UserTokenPolicy,
    ReadValueId,
    Variant,
};

/// The server endpoint we connect to (matches the server's bind address).
const SERVER_URL: &str = "opc.tcp://localhost:4840/";

/// The line name prefix used when the server built its NodeIds.
const LINE: &str = "Line1";

#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();

    // 1. Build a client. Anonymous, no security - matches the server's
    //  unsecured "none" endpoint. `trust_server_certs(true)` means we accept
    //  the server's self-signed cert without manual trust (dev only).
    let mut client = ClientBuilder::new()
        .application_name("SFP Smart Factory OPC-UA Client")
        .application_uri("urn:smart-factory:client")
        .product_uri("urn:smart-factory:client")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(3)
        .client()
        .unwrap();

    // 2. Connect and create a session. This opens a secure channel and
    //  performs the OPC-UA handshake, returning a session plus an event
    //  loop that must be driven (spawned) for the session to stay alive.
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                SERVER_URL,
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await
        .unwrap();

    // The event loop runs in the background, handling keep-alives,
    // reconnects, and delivering subscription notifications.
    let handle = event_loop.spawn();
    session.wait_for_connection().await;
    println!("Connected to {SERVER_URL}");

    if let Err(status) = subscribe(session.clone()).await {
        eprintln!("Failed to subscribe: {status}");
        let _ = session.disconnect().await;
        return Err(());
    }

    // Graceful shutdown: close the session on Ctrl-C so the server is told.
    let session_c = session.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("Failed to register Ctrl-C handler: {e}");
            return;
        }
        let _ = session_c.disconnect().await;
    });

    // Block until the event loop ends (e.g. after disconnect).
    let _ = handle.await;
    Ok(())
}

/// Create a subscription and monitor the line's nodes.
async fn subscribe(session: Arc<Session>) -> Result<(), StatusCode> {
    // 3. Create a subscription. The server will batch notifications and push
    //  them roughly every `publishing_interval`. The callback runs for each
    //  monitored-item change the server reports.
    // Resolve our namespace index at runtime instead of hardcoding it.
    let ns = resolve_namespace_index(&session, "urn:smart-factory:sfp").await?;
    let subscription_id = session
        .create_subscription(
            Duration::from_millis(500), // publishing interval
            10,                              // lifetime-count
            30,                        // max keep-alive count
            0,                // max notifications per publish (0 = no limit)
            0,                                     // priority
            true,                                          // publishing enabled
            DataChangeCallback::new(|value, item| print_change(&value, item)),                                         
        )
        .await?;
    println!("Created subscription id = {subscription_id}");

    // 4. Register monitored items - the specific nodes we care about. These
    //  NodeIds must match exactly how the server constructed them.
    let node_names = [
        format!("{LINE}/HeaterZone1/Temperature"),
        format!("{LINE}/HeaterZone2/Temperature"),
        format!("{LINE}/Sheet/Moisture"),
        format!("{LINE}/Checkweigher/Weight"),
        format!("{LINE}/State"),
        format!("{LINE}/Throughput"),
    ];

    let items: Vec<MonitoredItemCreateRequest> = node_names
        .iter()
        .map(|name| NodeId::new(ns, name.clone()).into())
        .collect();

    let result = session
        .create_monitored_items(subscription_id, TimestampsToReturn::Both, items)
        .await?;

    // Inspect the PER-ITEM status. The overall call can succeed while
    // individual items fail (e.g. BadNodeIdUnknown if a NodeId doesn't match
    // anything on the server). Without this check, failures are invisible.
    for (name, created) in node_names.iter().zip(result.iter()) {
        let status = created.result.status_code;
        if status.is_good() {
            println!("  monitoring OK: {name}");
        } else {
            println!("  FAILED to monitor {name}: {status}");
        }
    }
    println!(
        "Monitoring {} nodes. Waiting for changes...\n",
        node_names.len()
    );
    Ok(())
}

/// Print one data-change notification, surfacing the OPC-UA status so quality
/// degradation (Bad / Uncertain) is visible - the whole point of carrying
/// quality through the pipeline.
fn print_change(value: &DataValue, item: &MonitoredItem) {
    let node_id = &item.item_to_monitor().node_id;
    let status = value.status.unwrap_or(StatusCode::Good);

    match &value.value {
        Some(v) => println!("{node_id:<40} = {v:?}  [{status}]"),
        None => println!("{node_id:<40} = <no value>    [{status}]"),
    }
}

/// Resolve the namespace index for `uri` at runtime by reading the server's
/// standard `Server_NamespaceArray` node (NodeId `i=2255`, namespace 0).
/// 
/// The NamespaceArray is an array of namespace URIs where the array position
/// IS the namespace index. Reading it and searching for our URI means the
/// client never hardcodes an index that could shift if the server's node
/// manager layout changes. This is how production OPC-UA clients do it.
async fn resolve_namespace_index(
    session: &Session,
    uri: &str,
) -> Result<u16, StatusCode> {
    // NodeId i=2255 in namespace 0 is Server_NamespaceArray, a well-known node.
    let namespace_array_node = NodeId::new(0u16, 2255u32);

    let to_read = ReadValueId::from(namespace_array_node);
    let results = session
        .read(&[to_read], TimestampsToReturn::Neither, 0.0)
        .await
        .map_err(|e| e.status())?;

    let value = results
        .into_iter()
        .next()
        .and_then(|dv| dv.value)
        .ok_or(StatusCode::BadNoData)?;

    // The value is an array of strings. Find the position of our URI; that
    // position is the namespace index.
    let Variant::Array(array) = value else {
        return Err(StatusCode::BadTypeMismatch);
    };

    for (index, element) in array.values.iter().enumerate() {
        if let Variant::String(s) = element {
            if s.as_ref() == uri {
                return Ok(index as u16);
            }
        }
    }

    // URI not present in the server's namespace table.
    Err(StatusCode::BadNotFound)
}