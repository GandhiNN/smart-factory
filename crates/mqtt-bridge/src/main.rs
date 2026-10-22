//! MQTT bridge: the OT->IT edge gateway.
//! 
//! Subscribes to the simulated SFP maker line over OPC-UA and republishes each data change to an MQTT broker.
//! The set of nodes to monitor is DISCOVERED by browsing the 
//! server's address space at runtime - the bridge is never told
//! the line's shape in advance.
//! This is how real OPC-UA clients work
//! 
//! Plain-topic / JSON payload; Sparkplug B comes later

use std::sync::Arc;
use std::time::Duration;

use log::{error, info, warn};
use opcua::client::services::Browse;
use opcua::client::{
    ClientBuilder,
    DataChangeCallback,
    IdentityToken,
    MonitoredItem,
    Session,
    UARequest,
};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    BrowseDescription,
    BrowseDirection,
    BrowseResultMask,
    DataValue,
    Identifier,
    MessageSecurityMode,
    MonitoredItemCreateRequest,
    NodeClass,
    NodeClassMask,
    NodeId,
    ReferenceTypeId,
    StatusCode,
    TimestampsToReturn,
    UserTokenPolicy,
    Variant,
};
use rumqttc::{
    AsyncClient,
    MqttOptions,
    QoS
};

/// OPC-UA server endpoint (the source of plant-floor data).
/// Use an explicit IPv4 address, not `localhost`
const SERVER_URL: &str = "opc.tcp://127.0.0.1:4840/";

/// The namespace URI our line lives under on the server.
const NAMESPACE_URI: &str = "urn:smart-factory:sfp";

/// The line name prefix used when the server built its NodeIds.
const LINE: &str = "Line1";

/// MQTT broker connection details.
const MQTT_HOST: &str = "127.0.0.1";
const MQTT_PORT: u16 = 1883;
const MQTT_CLIENT_ID: &str = "sfp-mqtt-bridge";

/// Root of the topic tree we publish under: `smart-factory/<node-identifier>`
const TOPIC_ROOT: &str = "smart-factory";

#[tokio::main]
async fn main() -> Result<(), ()> {
    env_logger::init();

    // 1. Connect to the MQTT broker. Spawn the event loop or nothing sends.
    let mut mqtt_options = MqttOptions::new(MQTT_CLIENT_ID, MQTT_HOST, MQTT_PORT);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqtt_options, 32);

    tokio::spawn(async move {
        loop {
            match mqtt_eventloop.poll().await {
                Ok(_event) => {}
                Err(e) => {
                    warn!("MQTT event loop error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // 2. Connect to the OPC-UA server.
    let mut client = ClientBuilder::new()
        .application_name("SFP Smart Factory MQTT Bridge")
        .application_uri("urn:smart-factory:mqtt-bridge")
        .product_uri("urn:smart-factory:mqtt-bridge")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(3)
        .client()
        .unwrap();

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

    let handle = event_loop.spawn();
    session.wait_for_connection().await;
    println!("Connected to OPC-UA server {SERVER_URL}");
    println!("Publishing to MQTT broker {MQTT_HOST}:{MQTT_PORT} under '{TOPIC_ROOT}/'");

    if let Err(status) = subscribe(session.clone(), mqtt_client).await {
        error!("Failed to subscribe: {status}");
        let _ = session.disconnect().await;
        return Err(());
    }

    // Graceful shutdown on Ctrl-C.
    let session_c = session.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("Failed to register Ctrl-C handler: {e}");
            return;
        }
        let _ = session_c.disconnect().await;
    });

    let _ = handle.await;
    Ok(())
}

/// Create a subscription and monitor every variable discovered under the line folder. 
/// The callback publishes each change to MQTT.
async fn subscribe(session: Arc<Session>, mqtt_client: AsyncClient) -> Result<(), StatusCode> {
    // Resolve our namespace index using the built-in helper
    // (reads and caches the server's NamespaceArray for us)

    let ns = session
        .get_namespace_index(NAMESPACE_URI)
        .await
        .map_err(|e| e.status())?;

    // Discover the variable nodes by browsing the server - no hardcoded list.
    let nodes = discover_variables(&session, ns).await?;
    if nodes.is_empty() {
        warn!("Discovered no variables under the line folder; nothing to monitor.");
    }

    let subscription_id = session
        .create_subscription(
            Duration::from_millis(500), // publishing interval
            10, // lifetime count
            30, // max keep-alive count
            0,  // max notifications per publish (0 = no limit)
            0, // priority
            true,   // publishing enabled
            DataChangeCallback::new(move | value, item| {
                publish_change(&mqtt_client, &value, item);
            }),
        )
        .await?;
    println!("Created subscription id = {subscription_id}");

    let items: Vec<MonitoredItemCreateRequest> = nodes
        .iter()
        .cloned()
        .map(|n| n.into()).collect();

    let result = session
        .create_monitored_items(
            subscription_id, 
            TimestampsToReturn::Both, 
            items)
        .await?;

    for (node, created) in nodes.iter().zip(result.iter()) {
        let status = created.result.status_code;
        if status.is_good() {
            println!("  monitoring OK: {node}");
        } else {
            println!("  FAILED to monitor {node}: {status}");
        }
    }
    println!("Monitoring {} discovered nodes. Bridging to MQTT...\n", nodes.len());
    Ok(())
}

/// Browse the server to discover all Variable nodes under the line folder.
/// 
/// The line folder is `NodeId(ns, "Line1")` (the server built it that way).
/// We ask for forward hierarchical references and keep only those whose target
/// NodeClass is Variable, returning their NodeIds. This decouples the bridge
/// from the line's shape entirely: add a signal to the server's config and the
/// bridge picks it up with no code change.
async fn discover_variables(session: &Session, ns: u16) -> Result<Vec<NodeId>, StatusCode> {
    let line_folder = NodeId::new(ns, LINE);

    let to_browse = BrowseDescription {
        node_id: line_folder,
        browse_direction: BrowseDirection::Forward,
        reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
        include_subtypes: true,
        node_class_mask: NodeClassMask::VARIABLE.bits(),
        result_mask: BrowseResultMask::All as u32,
    };

    let response = Browse::new(session)
        .browse_node(to_browse)
        .send(session.channel())
        .await
        .map_err(|e| e.status())?;

    let mut nodes = Vec::new();
    if let Some(results) = response.results {
        for result in results {
            if let Some(refs) = result.references {
                for r in refs {
                    if r.node_class == NodeClass::Variable {
                        // ExpandedNodeId -> NodeId (local server, so this is direct).
                        nodes.push(r.node_id.node_id);
                    }
                }
            }
        }
    }

    info!("Browse discovered {} variable node(s) under {LINE}", nodes.len());
    Ok(nodes)
}

/// Publish one data-change notification to MQTT.
/// 
/// Topic:  smart-factory/<node-identifier> e.g. smart-factory/Line1/HeaterZone1/Temperature
/// Payload: JSON { "value": <v>, "status": "<status>", "ts": "<source_timestamp>" }
fn publish_change(mqtt_client: &AsyncClient, value: &DataValue, item: &MonitoredItem) {
    let node_id = &item.item_to_monitor().node_id;

    // NodeId identifier is a string like "Line1/HeaterZone1/Temperature".


    let identifier = match &node_id.identifier {
        Identifier::String(s) => s.to_string(),
        other => other.to_string(), // numeric/guid/opaque: fall back to tagged form
    };
    let topic = format!("{TOPIC_ROOT}/{identifier}");

    let status = value.status.unwrap_or(StatusCode::Good);

    // Render the value as a serde_json value where we can; fall back to string.
    let json_value = variant_to_json(value.value.as_ref());

    let ts = value
        .source_timestamp
        .map(|t| t.to_string())
        .unwrap_or_default();

    let payload = serde_json::json!({
        "value": json_value,
        "status": &status.to_string(),
        "ts": ts,
    });

    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to encode payload for {topic}: {e}");
            return;
        }
    };

    if let Err(e) = mqtt_client.try_publish(
        &topic,
        QoS::AtLeastOnce,
        false,
        bytes) {
            warn!("Failed to publish to {topic}: {e}");
    }
}

/// Convert an OPC-UA Variant into a JSON value for the payload.
/// Numeric types stays numeric; everything else degrades to a string so we never lose data.
fn variant_to_json(v: Option<&Variant>) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Some(Variant::Double(n)) => serde_json::json!(n),
        Some(Variant::Float(n)) => serde_json::json!(n),
        Some(Variant::Int64(n)) => serde_json::json!(n),
        Some(Variant::Int32(n)) => serde_json::json!(n),
        Some(Variant::UInt64(n)) => serde_json::json!(n),
        Some(Variant::UInt32(n)) => serde_json::json!(n),
        Some(Variant::Boolean(b)) => serde_json::json!(b),
        Some(other) => Value::String(format!("{other:?}")),
        None => Value::Null,
    }
}
