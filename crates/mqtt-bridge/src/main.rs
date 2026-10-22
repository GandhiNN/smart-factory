//! MQTT bridge: the OT->IT edge gateway.
//! 
//! Subscribes to the simulated SFP maker line over OPC-UA (the exact same
//! subscription mechanics as `opcua-client`) and, instead of printing each
//! data change, PUBLISHES it to an MQTT broker. This is the real hand-off 
//! from the plant-floor protocol (OPC-UA) to the message-bus world (MQTT
//! -> Kafka -> analytics).
//! 
//! Plain-topic / JSON payload first; Sparkplug B comes later.

use std::sync::Arc;
use std::time::Duration;

use log::{error, warn};
use opcua::client::{
    ClientBuilder,
    DataChangeCallback,
    IdentityToken,
    MonitoredItem,
    Session,
};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    DataValue,
    MessageSecurityMode,
    MonitoredItemCreateRequest,
    NodeId,
    ReadValueId,
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
const SERVER_URL: &str = "opc.tcp://localhost:4840/";

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

    // 1. Connect to the MQTT broker. `AsyncClient` is what we publish with;
    //  the `EventLoop` must be polled continuously or nothing is actually
    //  sent - publishes just queue. So we spawn a task to drive it.
    let mut mqtt_options = MqttOptions::new(MQTT_CLIENT_ID, MQTT_HOST, MQTT_PORT);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqtt_options, 32);

    tokio::spawn(async move {
        loop {
            match mqtt_eventloop.poll().await {
                Ok(_event) => {
                    /* connection events; ignore for now */
                }
                Err(e) => {
                    // rumqttc auto-reconnects; log and keep polling.
                    warn!("MQTT event loop error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // 2. Connect to the OPC-UA server (same as opcua-client).
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

/// Create a subscription and monitor the line's nodes. Each data change is
/// published to MQTT by the callback (which captures a clone of the MQTT client).
async fn subscribe(session: Arc<Session>, mqtt_client: AsyncClient) -> Result<(), StatusCode> {
    let ns = resolve_namespace_index(&session, NAMESPACE_URI).await?;

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
        .create_monitored_items(
            subscription_id, 
            TimestampsToReturn::Both, 
            items)
            .await?;
    
    for (name, created) in node_names.iter().zip(result.iter()) {
        let status = created.result.status_code;
        if status.is_good() {
            println!("  monitoring OK: {name}");
        } else {
            println!("  FAILED to monitor {name}: {status}");
        }
    }

    println!("Monitoring {} nodes. Bridging to MQTT...\n", node_names.len());

    Ok(())
}


/// Publish one data-change notification to MQTT.
/// 
/// Topic:  smart-factory/<node-identifier> e.g. smart-factory/Line1/HeaterZone1/Temperature
/// Payload: JSON { "value": <v>, "status": "<status>", "ts": "<source_timestamp>" }
/// 
/// Uses `try_publish`, which is non-blocking and safe to call from this
/// synchronous callback: it enqeueues onto the MQTT event loop we spawned.
fn publish_change(mqtt_client: &AsyncClient, value: &DataValue, item: &MonitoredItem) {
    let node_id = &item.item_to_monitor().node_id;

    // NodeId identifier is a string like "Line1/HeaterZone1/Temperature".
    let identifier = node_id.identifier.to_string();
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

/// Resolve the namespace index for `uri` at runtime via Server_NamespaceArray
/// (NodeId i=2255). Same approach as opcua-client.
async fn resolve_namespace_index(session: &Session, uri: &str) -> Result<u16, StatusCode> {
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
    Err(StatusCode::BadNotFound)
}