use crate::bevy_tokio_tasks::{TokioTasksPlugin, TokioTasksRuntime};
use async_nats::jetstream::kv::Operation;
use bevy::prelude::*;
use bevygap_shared::nats::{cert_digest_lookup_keys, *};
use bevygap_shared::protocol::DeploymentMetrics;
use futures::StreamExt;
use lightyear::connection::client::{Connected, Disconnected};
use lightyear::connection::server::Start;
use lightyear::connection::shared::{ConnectionRequestHandler, DeniedReason};
use lightyear::netcode::NetcodeServer;
use lightyear::prelude::server::{ClientOf, WebTransportServerIo};
use lightyear::prelude::*;
use log::{debug, error, info, warn};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::arbitrium_env::ArbitriumEnv;
use crate::edgegap_context::{self, ArbitriumContext};

/// Plugin for gameservers that run on edgegap.
/// TODO We need to know if the cert is self signed or not - if so, we can extract the cert digest
/// and tell the browser to use it.
/// If not, and it's a trusted cert, do nothing.
pub struct BevygapServerPlugin;

#[derive(Resource)]
struct CertDigest(String);

#[derive(Resource, Default)]
struct BevygapReadiness {
    published: bool,
    last_missing: Option<String>,
}

#[derive(Event)]
pub struct NatsConnected;

#[derive(Event)]
pub struct BevygapReady;

impl Plugin for BevygapServerPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<TokioTasksPlugin>() {
            app.add_plugins(TokioTasksPlugin::default());
        }
        // Load the Edgegap ENVs
        info!("Reading Arbitrium ENVs");
        let arb_env = ArbitriumEnv::from_env().expect("Failed to read Arbitrium ENVs");
        app.insert_resource(arb_env);

        // When using a self-signed cert for your NATS server, the server needs the root CA .pem file
        // in order to verify the server's certificate. Since this file is around 2kB, and Edgegap
        // limits you to 255 bytes in ENV vars, we set this from a command line arg instead.
        // (Edgegap have a bug report in the backlog to increase this limit)
        //
        // If present, we write the contents to an ENV var, which is later read by setup_nats().
        // In future, we hope to just set this ENV var directly in the Edgegap Dashboard.
        inject_ca_root_env_var_from_cmdline_arg();

        app.add_systems(Startup, setup_nats);
        app.init_resource::<BevygapReadiness>();
        app.add_systems(Update, publish_readiness_to_nats);

        app.add_observer(extract_cert_digest);
        app.add_observer(edgegap_context::fetch_context_on_nats_connected);
        app.add_observer(setup_connection_request_handler);

        app.add_observer(handle_lightyear_client_connect);
        app.add_observer(handle_lightyear_client_disconnect);
        app.add_observer(handle_deployment_metrics_update);
    }
}

#[allow(unreachable_patterns)]
fn extract_cert_digest(
    trigger: On<Add, WebTransportServerIo>,
    webtransport_servers: Query<&WebTransportServerIo>,
    mut commands: Commands,
) {
    let Ok(webtransport_server) = webtransport_servers.get(trigger.entity) else {
        return;
    };
    let certificate_chain = webtransport_server.certificate.certificate_chain();
    let Some(certificate) = certificate_chain.as_slice().first() else {
        warn!("WebTransport server had no certificate chain; cannot publish cert digest");
        return;
    };
    let digest = certificate.hash().to_string();
    info!("Extracted cert digest: {}", digest);
    commands.insert_resource(CertDigest(digest));
}

/// If --ca_contents XXXXXX present on command line, set NATS_CA_CONTENTS to XXXXXX
fn inject_ca_root_env_var_from_cmdline_arg() {
    use std::env;
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        return;
    }
    let mut found_flag = false;
    for arg in args {
        if found_flag {
            let ca_root = arg.clone();
            info!(
                "Found --ca_contents, setting NATS_CA_CONTENTS to [{} bytes]",
                ca_root.len()
            );
            env::set_var("NATS_CA_CONTENTS", ca_root);
            return;
        }
        if arg == "--ca_contents" {
            found_flag = true;
            continue;
        }
    }
}

// switch to observers for ConnectEvent and DisconnectEvent!

fn handle_lightyear_client_disconnect(
    trigger: On<Add, Disconnected>,
    clients: Query<&RemoteId, With<ClientOf>>,
    nats_sender: ResMut<NatsSender>,
) {
    let Ok(client_id) = clients.get(trigger.entity).map(|id| id.0.to_bits()) else {
        return;
    };
    info!("Lightyear disconnect event for client_id {}", client_id);
    nats_sender.client_disconnected(client_id);
}

fn handle_lightyear_client_connect(
    trigger: On<Add, Connected>,
    clients: Query<&RemoteId, With<ClientOf>>,
    nats_sender: ResMut<NatsSender>,
) {
    let Ok(client_id) = clients.get(trigger.entity).map(|id| id.0.to_bits()) else {
        return;
    };
    info!("Lightyear connect event for client_id {}", client_id);
    nats_sender.client_connected(client_id);
}

/// We create a BevygapConnectionRequestHandler and store it in a resource.
/// This is handed to lightyear, and used to accept or deny incoming client connections.
fn setup_connection_request_handler(
    _trigger: On<NatsConnected>,
    bgnats: Res<BevygapNats>,
    runtime: ResMut<TokioTasksRuntime>,
    mut servers: Query<(Entity, &mut NetcodeServer)>,
    mut commands: Commands,
) {
    // we store this in a resource, because we'll need to push new data into it
    let valid_client_ids = Arc::new(RwLock::new(HashSet::new()));
    let crh = BevygapConnectionRequestHandler::new(valid_client_ids.clone());
    let arc_crh: Arc<dyn ConnectionRequestHandler> = Arc::new(crh);
    watch_valid_client_ids(runtime, bgnats.clone(), valid_client_ids);
    let mut installed = false;
    for (entity, mut netcode_server) in &mut servers {
        netcode_server.set_connection_request_handler(arc_crh.clone());
        commands.trigger(Start { entity });
        installed = true;
        info!("Installed Bevygap connection-request handler on Lightyear server {entity}");
    }
    if !installed {
        warn!("No Lightyear NetcodeServer found; Bevygap connection-request handler was not installed");
    }
    commands.insert_resource(CRH(arc_crh));
}

fn watch_valid_client_ids(
    runtime: ResMut<TokioTasksRuntime>,
    bgnats: BevygapNats,
    valid_client_ids: Arc<RwLock<HashSet<u64>>>,
) {
    runtime.spawn_background_task(|_ctx| async move {
        loop {
            let mut entries = match bgnats.kv_c2s().watch_all().await {
                Ok(entries) => entries,
                Err(error) => {
                    error!("Failed to watch Bevygap client/session KV: {error}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            info!("Watching Bevygap issued client ids");
            while let Some(entry) = entries.next().await {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        error!("Error while watching Bevygap client/session KV: {error}");
                        continue;
                    }
                };
                let Ok(client_id) = entry.key.parse::<u64>() else {
                    warn!("Ignoring non-u64 Bevygap client id key: {}", entry.key);
                    continue;
                };
                let Ok(mut ids) = valid_client_ids.write() else {
                    error!("Bevygap valid-client-id lock poisoned; watcher will retry");
                    break;
                };
                match entry.operation {
                    Operation::Put => {
                        ids.insert(client_id);
                        info!("Bevygap client id {client_id} is now admissible");
                    }
                    Operation::Delete | Operation::Purge => {
                        ids.remove(&client_id);
                        info!("Bevygap client id {client_id} is no longer admissible");
                    }
                }
            }
            warn!("Bevygap client/session KV watcher exited; restarting");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// Publish server readiness once all async startup pieces are present.
///
/// NATS connection, Edgegap context loading, and WebTransport certificate
/// creation complete through different Bevy/async paths. A one-shot observer
/// can miss one of them; this system retries quietly until all prerequisites are
/// present, then publishes the metadata exactly once.
fn publish_readiness_to_nats(
    context: Option<Res<ArbitriumContext>>,
    digest: Option<Res<CertDigest>>,
    bgnats: Option<Res<BevygapNats>>,
    nats_sender: Option<Res<NatsSender>>,
    mut readiness: ResMut<BevygapReadiness>,
    mut commands: Commands,
) {
    if readiness.published {
        return;
    }

    let mut missing = Vec::new();
    if context.is_none() {
        missing.push("Edgegap context");
    }
    if digest.is_none() {
        missing.push("WebTransport certificate digest");
    }
    if bgnats.is_none() {
        missing.push("NATS connection");
    }
    if nats_sender.is_none() {
        missing.push("NATS sender");
    }

    if !missing.is_empty() {
        let missing = missing.join(", ");
        if readiness.last_missing.as_deref() != Some(missing.as_str()) {
            info!("Waiting to publish Bevygap readiness; missing: {missing}");
            readiness.last_missing = Some(missing);
        }
        return;
    }

    let context = context.expect("checked above");
    let digest = digest.expect("checked above");
    let nats_sender = nats_sender.expect("checked above");

    info!("CONTEXT added: {context:?}");
    info!("CONTEXT fqdn: {}", context.fqdn());
    let request_id = context.request_id();
    let public_ip = context.public_ip();
    let external_port = context.game_external_port();
    let cert_digest_keys =
        cert_digest_lookup_keys(Some(request_id.as_str()), public_ip.as_str(), external_port);
    info!(
        "Publishing cert digest for request_id={request_id}, endpoint={public_ip}:{:?}, keys={}",
        external_port,
        cert_digest_keys.join(",")
    );
    nats_sender.cert_digest(cert_digest_keys, digest.0.clone());
    nats_sender.arbitrium_context((*context).clone());
    readiness.published = true;
    commands.trigger(BevygapReady);
}

#[derive(Debug, Event)]
enum NatsEvent {
    ClientConnected(u64),
    ClientDisconnected(u64),
    ArbitriumContext(ArbitriumContext),
    CertDigest(Vec<String>, String),
    DeploymentMetrics(DeploymentMetrics),
}

#[derive(Resource)]
struct NatsSender(tokio::sync::mpsc::UnboundedSender<NatsEvent>);

impl NatsSender {
    fn client_connected(&self, client_id: u64) {
        if let Err(error) = self.0.send(NatsEvent::ClientConnected(client_id)) {
            error!("Unable to send NatsEvent for client_connected: {error:?}");
        }
    }

    fn client_disconnected(&self, client_id: u64) {
        if let Err(error) = self.0.send(NatsEvent::ClientDisconnected(client_id)) {
            error!("Unable to send NatsEvent for client_disconnected: {error:?}");
        }
    }

    fn arbitrium_context(&self, context: ArbitriumContext) {
        if let Err(error) = self.0.send(NatsEvent::ArbitriumContext(context)) {
            error!("Unable to send NatsEvent for arbitrium_context: {error:?}");
        }
    }

    fn cert_digest(&self, keys: Vec<String>, digest: String) {
        if let Err(error) = self.0.send(NatsEvent::CertDigest(keys, digest)) {
            error!("Unable to send NatsEvent for cert_digest: {error:?}");
        }
    }

    fn deployment_metrics(&self, metrics: DeploymentMetrics) {
        if let Err(error) = self.0.send(NatsEvent::DeploymentMetrics(metrics)) {
            error!("Unable to send NatsEvent for deployment_metrics: {error:?}");
        }
    }
}

#[derive(Debug, Event, Clone)]
pub struct BevygapDeploymentMetrics {
    pub total_players: u32,
    pub max_players: u32,
    pub max_rooms: u32,
    pub cpu_percent: Option<f32>,
    pub rooms: Vec<bevygap_shared::protocol::DeploymentRoomMetrics>,
}

fn handle_deployment_metrics_update(
    trigger: On<BevygapDeploymentMetrics>,
    context: Option<Res<ArbitriumContext>>,
    nats_sender: Option<Res<NatsSender>>,
) {
    let Some(context) = context else {
        return;
    };
    let Some(nats_sender) = nats_sender else {
        return;
    };
    let metrics = trigger.event();
    let deployment_metrics = DeploymentMetrics {
        request_id: context.request_id(),
        public_ip: context.public_ip(),
        external_port: context.game_external_port(),
        total_players: metrics.total_players,
        max_players: metrics.max_players,
        max_rooms: metrics.max_rooms,
        cpu_percent: metrics.cpu_percent,
        rooms: metrics.rooms.clone(),
    };
    nats_sender.deployment_metrics(deployment_metrics);
}

/// Exists purely to allow us to trigger an event via command queue.
/// See setup_nats() below.
struct DeferredNatsConnectedCommand;

impl Command for DeferredNatsConnectedCommand {
    fn apply(self, world: &mut World) {
        world.trigger(NatsConnected);
    }
}

fn setup_nats(runtime: ResMut<TokioTasksRuntime>, mut commands: Commands) {
    info!("Setting up NATS");

    let (nats_event_sender, mut nats_event_receiver) =
        tokio::sync::mpsc::unbounded_channel::<NatsEvent>();
    commands.insert_resource(NatsSender(nats_event_sender));

    runtime.spawn_background_task(|mut ctx| async move {
        let mut attempt = 0_u32;
        let bgnats = loop {
            match BevygapNats::new_and_connect("bevygap_server_plugin").await {
                Ok(nats) => break nats,
                Err(error) => {
                    attempt += 1;
                    error!("Failed to setup NATS on attempt {attempt}: {error}; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        info!("NATS connected");

        let kv_c2s = bgnats.kv_c2s().clone();
        let kv_sessions = bgnats.kv_active_connections().clone();
        let kv_cert_digests = bgnats.kv_cert_digests().clone();
        let kv_deployment_metrics = bgnats.kv_deployment_metrics().clone();
        let client = bgnats.client().clone();

        ctx.run_on_main_thread(move |ctx| {
            ctx.world.insert_resource(bgnats);
            // main thread work is executed by TokioTasks plugin by removing the TokioTasksRuntime resource,
            // doing the work, then reinserting the resource.
            // if we use a trigger here, the observer will fire instantly, and run while the TokioTasksRuntime is not present in the world.
            // unfortunately, our observer requests the TokioTasksRuntime resource, and panics if it's not present.
            //
            // so we have to defer the trigger, by using a Command queue.
            // instead of:
            // ctx.world.trigger(NatsConnected);
            // we do:
            ctx.world.commands().queue(DeferredNatsConnectedCommand);
            // so the actual triggering happens after the TokioTasksRuntime resource is reinserted into the world.
        })
        .await;

        let mut client_id_to_session_id = HashMap::new();

        // Loop over nats_event_receiver and log received NatsEvents
        info!("Starting NatsEvent loop");
        loop {
            let Some(ev) = nats_event_receiver.recv().await else {
                error!("NatsEvent channel closed; Bevygap NATS event loop is stopping");
                break;
            };
            match ev {
                NatsEvent::ClientConnected(client_id) => {
                    info!("Client connected: {}, writing to nats kv", client_id);
                    match kv_c2s.get(client_id.to_string()).await {
                        Err(error) => {
                            error!(
                                "Failed to get session_id from KV for client_id {client_id}: {error}"
                            );
                        }
                        Ok(None) => {
                            error!("Client ID {client_id} is not mapped to a Bevygap session id");
                        }
                        Ok(Some(session_id)) => {
                            let session_id_key = match String::from_utf8(session_id.into()) {
                                Ok(value) => value,
                                Err(error) => {
                                    error!(
                                        "Session id for client_id {client_id} is not utf8: {error}"
                                    );
                                    continue;
                                }
                            };
                            info!("Client ID {client_id} associated with session id: {session_id_key}",);
                            client_id_to_session_id.insert(client_id, session_id_key.clone());
                            if let Err(error) = kv_sessions
                                .put(session_id_key.clone(), client_id.to_string().into())
                                .await
                            {
                                error!(
                                    "Failed to put active connection for session_id={session_id_key}, client_id={client_id}: {error}"
                                );
                            } else {
                                info!(
                                    "Active connection put: session_id={session_id_key}, client_id={client_id}"
                                );
                            }
                            // delete the mappings.
                            // this signifies the session
                            // let _ = kv_c2s.delete(client_id.to_string()).await;
                        }
                    }
                }
                NatsEvent::ClientDisconnected(client_id) => {
                    info!("Client disconnected: {}, writing to nats kv", client_id);
                    if let Some(session_id) = client_id_to_session_id.remove(&client_id) {
                        if let Err(error) = kv_sessions
                            .delete(session_id.as_str())
                            .await
                        {
                            error!(
                                "Failed to delete active connection for session_id={session_id}, client_id={client_id}: {error}"
                            );
                        } else {
                            info!(
                                "Active connection delete: session_id={session_id}, client_id={client_id}"
                            );
                        }
                    } else {
                        error!("Client disconnected but not found in client_id_to_session_id");
                    }
                }
                NatsEvent::ArbitriumContext(context) => {
                    info!("ArbitriumContext added: {context:?}");
                    let arb_context_bytes = context.to_bytes();
                    if let Err(error) = client
                        .publish(nats_subject_name("gameserver.contexts"), arb_context_bytes.into())
                        .await
                    {
                        error!("Failed to write context to NATS: {error}");
                    }
                }
                NatsEvent::CertDigest(keys, digest) => {
                    for key in keys {
                        info!("CertDigest added: {key} -> {digest}");
                        if let Err(error) = kv_cert_digests
                            .put(key, digest.clone().into())
                            .await
                        {
                            error!("Failed to put cert digest in KV: {error}");
                        }
                    }
                }
                NatsEvent::DeploymentMetrics(metrics) => {
                    let key = deployment_metrics_key(&metrics.request_id);
                    let payload = match serde_json::to_vec(&metrics) {
                        Ok(payload) => payload,
                        Err(error) => {
                            error!("Failed to serialize deployment metrics: {error}");
                            continue;
                        }
                    };
                    if let Err(error) = kv_deployment_metrics.put(key.clone(), payload.into()).await
                    {
                        error!("Failed to put deployment metrics for {key}: {error}");
                    } else {
                        debug!(
                            "Deployment metrics put: key={key}, players={}/{}, rooms={}/{}",
                            metrics.total_players,
                            metrics.max_players,
                            metrics.rooms.len(),
                            metrics.max_rooms
                        );
                    }
                }
            }
            if let Err(error) = client.flush().await {
                error!("Failed to flush NATS: {error}");
            }
        }
    });
}

// /// Reasons for denying a connection request
// #[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
// pub enum DeniedReason {
//     ServerFull,
//     Banned,
//     InternalError,
//     AlreadyConnected,
//     TokenAlreadyUsed,
//     InvalidToken,
//     Custom(String),
// }

// / Trait for handling connection requests from clients.
// pub trait ConnectionRequestHandler: Debug + Send + Sync {
//     /// Handle a connection request from a client.
//     /// Returns None if the connection is accepted,
//     /// Returns Some(reason) if the connection is denied.
//     fn handle_request(&self, client_id: ClientId) -> Option<DeniedReason>;
// }

#[derive(Resource)]
pub struct CRH(Arc<dyn ConnectionRequestHandler>);

/// Only accept connections where the ClientId is in NATS associated with a session.
#[derive(Clone, Debug)]
pub struct BevygapConnectionRequestHandler {
    valid_client_ids: Arc<RwLock<HashSet<u64>>>,
}

// this is set as a arc dyn trait object.
// perhaps we should push valid ClientIDs into this struct as they arrive in nats?
// or lookup as needed? we can keep an arc ref to this in the server plugin, and hand a clone
// of it for registering with lightyear.
//
// Reasons we might actually want to reply with:
// Full  - no, mm should prevent this? maybe we do want this, in case of lobbies and races when users pick a server.
// banned - no, mm prevents
// internal error - maybe
// already connected - if client id is already connected?
// token already used - same as already used client id?
// invalidtoken - yep, if not registered in nats
// custom(string) - hmm.
//
// we don't want this to block, so i think we need to push data into it so it's always ready.
impl BevygapConnectionRequestHandler {
    pub fn new(valid_client_ids: Arc<RwLock<HashSet<u64>>>) -> Self {
        Self { valid_client_ids }
    }
}

impl ConnectionRequestHandler for BevygapConnectionRequestHandler {
    fn handle_request(&self, client_id: PeerId) -> Option<DeniedReason> {
        info!("BevygapConnectionRequestHandler({client_id})");
        let PeerId::Netcode(client_id) = client_id else {
            warn!("Rejecting non-netcode Bevygap client id: {client_id}");
            return Some(DeniedReason::InvalidToken);
        };
        let Ok(ids) = self.valid_client_ids.read() else {
            error!("Bevygap valid-client-id lock poisoned; rejecting client id {client_id}");
            return Some(DeniedReason::InvalidToken);
        };
        if ids.contains(&client_id) {
            None
        } else {
            warn!("Rejecting client id {client_id}: no Bevygap session mapping exists");
            Some(DeniedReason::InvalidToken)
        }
    }
}
