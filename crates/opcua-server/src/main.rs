//! OPC-UA server exposing a simulated SFP maker line.
//! 
//! This is the protocol layer. It owns a `MakerLine` from `sfp-sim-core`,
//! builds an OPC-UA address space that mirrors the line's structure, runs the
//! simulation on a timer (the "PLC scan cycle"), and pushes updated values -
//! with translated quality - into the address space so subscribed clients see
//! them change.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use opcua::client::ANONYMOUS_USER_TOKEN_ID;
use opcua::server::address_space::Variable;
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{
    simple_node_manager, InMemoryNodeManager, SimpleNodeManager, SimpleNodeManagerImpl
};
use opcua::server::{ServerBuilder, ServerEndpoint, SubscriptionCache};
use opcua::types::{DataValue, NodeId};
use rand::rngs::StdRng;
use rand::SeedableRng;
use sfp_sim_core::MakerLine;

mod mapping;
use mapping::quality_to_status;

/// The application URI, used to identify the server application itself.
/// The diagnostic node manager claims a namespace equal to this URI, so our
/// own node manager MUST NOT reuse it (see NAMESPACE_URI below).
const APPLICATION_URI: &str = "urn:smart-factory:sfp-server";

/// The namespace URI our simulated line lives under. For the simple node
/// manager this value decides node ownership, so it must be distinct from the
/// application URI (which the diagnostic node manager owns). If they collide,
/// the diagnostic manager - registered first - wins ownership of the namespace
/// and CreateMonitoredItems/Read for our nodes route to it, yielding
/// BadServiceUnsupported.
const NAMESPACE_URI: &str = "urn:smart-factory:sfp";

/// How often the simulation advances one tick (the "PLC scan cycle")
const TICK: Duration = Duration::from_millis(500);


#[tokio::main]
async fn main() {
    env_logger::init();

    // The process we are exposing. Protocol code owns the domain model.
    let line = MakerLine::new("Line1");

    // Bind host = the local interface to listen on. 0.0.0.0 = all
    // interfaces, which is what makes the server reachable from outside
    // the VM. Override with OPCUA_BIND_HOST if you need to pin one NIC.
    let bind_host = env_or("OPCUA_BIND_HOST", "0.0.0.0");

    // Advertised host = the address the server puts in its discovery response
    // and endpoint URLs. Remote clients RECONNECT to this, so on the VM it MUST
    // be a hostname/IP the client can resolve and reach (e.g. the VM's DNS name or
    // public IP). Defaults to localhost for local dev. Set OPCUA_ADVERTISED_HOST
    // on the VM.
    let advertised_host = env_or("OPCUA_ADVERTISED_HOST", "localhost");
    let port: u16 = env_or("OPCUA_PORT", "4840").parse().unwrap_or(4840);
    let discovery_url = format!("opc.tcp://{advertised_host}:{port}/");
    info!("Binding to {bind_host}:{port}, advertising {discovery_url}");

    // Build the OPC-UA server with one in-memory node manager for our namespace.
    let (server, handle) = ServerBuilder::new()
        .application_name("SFP Smart Factory OPC-UA Server")
        .application_uri(APPLICATION_URI)
        .product_uri(APPLICATION_URI)
        .create_sample_keypair(true)
        .trust_client_certs(true)
        .host(bind_host.clone())
        .port(port)
        .discovery_urls(vec![discovery_url.clone()])
        .add_endpoint(
            "none", ServerEndpoint::new_none("/", &[ANONYMOUS_USER_TOKEN_ID.to_owned()]),
        )
        .with_node_manager(simple_node_manager(
            NamespaceMetadata { 
                namespace_uri: NAMESPACE_URI.to_owned(),
                ..Default::default()
            },
            "sfp",
        ))
        .build()
        .unwrap();

    let node_manager = handle
        .node_managers()
        .get_of_type::<SimpleNodeManager>()
        .unwrap();
    let ns = handle.get_namespace_index(NAMESPACE_URI).unwrap();
    info!("Custom namespace '{NAMESPACE_URI}' has index {ns}");
    info!("Node manager owns namespaces: {:?}", node_manager.namespaces());

    // Build the address space to match the line, and start the update loop.
    build_address_space(ns, &node_manager, &line);
    tokio::spawn(run_simulation(
        ns,
        node_manager,
        handle.subscriptions().clone(),
        line,
    ));

    // Graceful shutdown on Ctrl-C so clients are told the server is going away.
    let handle_c = handle.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("Failed to register Ctrl-C handler: {e}");
            return;
        }
        handle_c.cancel();
    });

    info!("OPC-UA server starting, advertising {discovery_url} (namespace {NAMESPACE_URI})");

    // Runs until cancelled. This is the server's main loop.
    server.run().await.unwrap();
}

/// NodeId for an analog signal's value, derived from its index on the line.
/// We use string NodeIds so a browsing client sees meaningful identifiers.
fn signal_node_id(ns: u16, line_name: &str, signal_name: &str) -> NodeId {
    NodeId::new(ns, format!("{line_name}/{signal_name}"))
}

/// NodeId for the machine state variable.
fn state_node_id(ns: u16, line_name: &str) -> NodeId {
    NodeId::new(ns, format!("{line_name}/State"))
}

// NodeId for the throughput counter variable.
fn throughput_node_id(ns: u16, line_name: &str) -> NodeId {
    NodeId::new(ns, format!("{line_name}/Throughput"))
}

/// Run the simulation forever: each tick, advance the line, then push every
/// value into the address space with its translated OPC-UA status. Clients
/// subscribed to these nodes are notified of the changes.
async fn run_simulation(
    ns: u16,
    manager: Arc<InMemoryNodeManager<SimpleNodeManagerImpl>>,
    subscriptions: Arc<SubscriptionCache>,
    mut line: MakerLine,
) {
    let mut rng = StdRng::from_entropy();
    let mut interval = tokio::time::interval(TICK);
    let line_name = line.name.clone();

    loop {
        interval.tick().await;

        // Advance the process one scan cycle.
        line.tick(&mut rng);

        // Build the batch of (node, value-with-status) updates for this tick.
        let mut updates: Vec<(NodeId, Option<opcua::types::NumericRange>, DataValue)> = Vec::new();

        for signal in &line.signals {
            let node = signal_node_id(ns, &line_name, &signal.name);
            let status = quality_to_status(signal.quality);
            updates.push((node, None, DataValue::new_now_status(signal.value, status)));
        }

        // State and throughput are always Good (they are machine facts, not
        // sensor readings that can fail).
        updates.push((
            state_node_id(ns, &line_name),
            None,
            DataValue::new_now(line.state.as_code()),
        ));
        updates.push((
            throughput_node_id(ns, &line_name),
            None,
            DataValue::new_now(line.throughput as i64),
        ));

        if let Err(e) = manager.set_values(
            &subscriptions,
            updates.iter().map(|(n, r, v)| (n, r.as_ref(), v.clone())),
        ) {
            warn!("Failed to set values: {e}");
        }
    }
}

/// Build the OPC-UA address space so it mirrors the structure of the line:
/// a folder for the line, one variable per analog signal, plus State and
/// Throughput. This runs once at startup; values are updated later.
fn build_address_space(
    ns: u16,
    manager: &Arc<InMemoryNodeManager<SimpleNodeManagerImpl>>,
    line: &MakerLine,
) {
    let address_space = manager.address_space();
    let mut address_space = address_space.write();

    // A folder representing the whole line, hung under the standard Objects folder.
    let line_folder = NodeId::new(ns, line.name.clone());
    address_space.add_folder(
        &line_folder,
        line.name.clone(),
        line.name.clone(),
        &NodeId::objects_folder_id(),
    );

    // One variable per analog signal. Initial values seed the address space;
    // the simulation loop overwrites them continuously.
    let mut variables = Vec::new();
    for signal in &line.signals {
        let node = signal_node_id(ns, &line.name, &signal.name);
        variables.push(Variable::new(
            &node,
            signal.name.clone(),
            signal.name.clone(),
            signal.value,
        ));
    }

    // Machine state (as an integer code) and throughput counter.
    variables.push(Variable::new(
        &state_node_id(ns, &line.name),
        "State",
        "State",
        line.state.as_code(),
    ));
    variables.push(Variable::new(
        &throughput_node_id(ns, &line.name),
        "Throughput",
        "Throughput",
        line.throughput as i64,
    ));

    let _ = address_space.add_variables(variables, &line_folder);

}

/// Read an environment variable, or return `default` if it is unset/empty.
fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => default.to_owned(),
    }
}