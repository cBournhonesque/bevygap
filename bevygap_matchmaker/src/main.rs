// use std::time::Duration;

// use async_nats::jetstream;
// use async_nats::jetstream::stream::Stream;
// use async_nats::jetstream::stream::StorageType;
use async_nats::Client;
use clap::Parser;
use edgegap_async::apis::applications_api::*;
use edgegap_async::apis::configuration::*;
use futures::stream::StreamExt;
use lightyear::netcode::PRIVATE_KEY_BYTES;
use log::*;
use tracing_subscriber::{layer::*, util::*};

use bevygap_shared::nats::*;

mod session_delete_worker;
mod session_reaper;
mod session_service;

use session_delete_worker::*;
use session_reaper::*;
use session_service::*;

mod session_request_streamer;

pub const MAX_SESSION_CREATION_SECONDS: u64 = 60;
const LIGHTRIDER_PROTOCOL_ID_ENV: &str = "LIGHTRIDER_PROTOCOL_ID";
const LIGHTRIDER_PRIVATE_KEY_ENV: &str = "LIGHTRIDER_PRIVATE_KEY";
const LIGHTRIDER_LEGACY_KEY_ENV: &str = "LIGHTRIDER_NETCODE_KEY";
const LIGHTRIDER_REQUIRE_PRODUCTION_NETCODE_ENV: &str = "LIGHTRIDER_REQUIRE_PRODUCTION_NETCODE";

fn edgegap_configuration(_settings: &Settings) -> Configuration {
    let key =
        std::env::var("EDGEGAP_API_KEY").expect("EDGEGAP_API_KEY environment variable is not set");
    Configuration {
        base_path: "https://api.edgegap.com".to_string(),
        api_key: Some(ApiKey { prefix: None, key }),
        ..Default::default()
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Settings {
    #[arg(long, default_value = "spacepit_server")]
    app_name: String,
    #[arg(long, default_value = "v0.0.1")]
    app_version: String,
    /// private key, in format 1,2,3,4..  which should be 32 u8s long (for signing lightyear tokens)
    #[arg(long, default_value = "")]
    lightyear_private_key: String,
    /// The lightyear protocol id (u64)
    #[arg(long, default_value = "0")]
    lightyear_protocol_id: u64,
    /// The webhook url for edgegap session creation events
    /// (should write to nats for you, see bevygap_webhook_sink)
    #[arg(long, default_value = None)]
    session_webhook_url: Option<String>,
    /// Use a local mock Edgegap session instead of calling the Edgegap API.
    ///
    /// This is intended for game integration smoke tests: the HTTPD, NATS request/reply,
    /// cert digest lookup, ConnectToken generation, and client token connect path remain real.
    #[arg(long, default_value = "false")]
    mock_edgegap: bool,
    /// Public IP to put in mock Edgegap deployments.
    #[arg(long, default_value = "127.0.0.1")]
    mock_public_ip: String,
    /// External game port to put in mock Edgegap deployments.
    #[arg(long, default_value_t = 7777)]
    mock_external_port: u16,
    /// Prefix for mock Edgegap session ids.
    #[arg(long, default_value = "mock-session")]
    mock_session_id_prefix: String,
    /// Deployment request id used by local mock sessions for cert digest lookup.
    #[arg(long, default_value = "local-lightrider")]
    mock_deployment_request_id: String,
    /// Artificial delay before reporting a mock session as ready.
    #[arg(long, default_value_t = 100)]
    mock_ready_delay_ms: u64,
}

impl Settings {
    fn parse_private_key(&self) -> [u8; PRIVATE_KEY_BYTES] {
        let value = if self.lightyear_private_key.trim().is_empty() {
            std::env::var(LIGHTRIDER_PRIVATE_KEY_ENV)
                .ok()
                .or_else(|| std::env::var(LIGHTRIDER_LEGACY_KEY_ENV).ok())
                .unwrap_or_default()
        } else {
            self.lightyear_private_key.clone()
        };
        if value.trim().is_empty() {
            return [0u8; PRIVATE_KEY_BYTES];
        }
        if value.contains(',') {
            return parse_comma_private_key(&value);
        }
        parse_hex_private_key(&value)
    }

    pub fn protocol_id(&self) -> u64 {
        if self.lightyear_protocol_id != 0 {
            return self.lightyear_protocol_id;
        }
        std::env::var(LIGHTRIDER_PROTOCOL_ID_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .trim()
                    .parse::<u64>()
                    .expect("Failed to parse LIGHTRIDER_PROTOCOL_ID")
            })
            .unwrap_or(0)
    }

    fn require_production_netcode(&self) -> bool {
        std::env::var(LIGHTRIDER_REQUIRE_PRODUCTION_NETCODE_ENV)
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    fn validate_netcode_identity(&self, private_key: [u8; PRIVATE_KEY_BYTES]) {
        let protocol_id = self.protocol_id();
        let uses_dev_identity = protocol_id == 0 || private_key == [0u8; PRIVATE_KEY_BYTES];

        if self.require_production_netcode() && uses_dev_identity {
            panic!(
                "{LIGHTRIDER_REQUIRE_PRODUCTION_NETCODE_ENV}=1 requires nonzero {LIGHTRIDER_PROTOCOL_ID_ENV} and {LIGHTRIDER_PRIVATE_KEY_ENV}"
            );
        }

        if uses_dev_identity {
            warn!(
                "Using development Lightyear netcode identity; set {LIGHTRIDER_PROTOCOL_ID_ENV} and {LIGHTRIDER_PRIVATE_KEY_ENV} for shared matchmaker/server production tokens"
            );
        } else {
            info!("Using configured Lightyear netcode identity with protocol id {protocol_id}");
        }
    }
}

fn parse_comma_private_key(value: &str) -> [u8; PRIVATE_KEY_BYTES] {
    let private_key: Vec<u8> = value
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',')
        .collect::<String>()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u8>()
                .expect("Failed to parse number in private key")
        })
        .collect();
    if private_key.len() != PRIVATE_KEY_BYTES {
        panic!(
            "Private key must contain exactly {} numbers",
            PRIVATE_KEY_BYTES
        );
    }
    let mut bytes = [0u8; PRIVATE_KEY_BYTES];
    bytes.copy_from_slice(&private_key);
    bytes
}

fn parse_hex_private_key(value: &str) -> [u8; PRIVATE_KEY_BYTES] {
    let normalized = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if normalized.len() != PRIVATE_KEY_BYTES * 2 {
        panic!("Hex private key must contain exactly 64 hex characters");
    }
    let mut bytes = [0u8; PRIVATE_KEY_BYTES];
    for (index, chunk) in normalized.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).expect("private key is not utf8");
        bytes[index] = u8::from_str_radix(hex, 16).expect("Failed to parse hex private key");
    }
    bytes
}

async fn watch_for_gameserver_announcements(
    state: &MatchmakerState,
) -> Result<(), async_nats::Error> {
    info!("Watching for gameserver announcements");
    let client = state.nats_client();
    let mut subscriber = client
        .subscribe(nats_subject_name("gameserver.contexts"))
        .await?;

    while let Some(message) = subscriber.next().await {
        info!("NEW GAMESERVER: {:?}", message);
    }
    info!("Gameserver announcement watcher exiting");
    Ok(())
}

#[derive(Clone)]
pub(crate) struct MatchmakerState {
    nats: BevygapNats,
    api_config: Configuration,
    settings: Settings,
    lypkey: [u8; PRIVATE_KEY_BYTES],
}

impl MatchmakerState {
    pub(crate) fn nats_client(&self) -> Client {
        self.nats.client()
    }
    pub(crate) fn configuration(&self) -> &Configuration {
        &self.api_config
    }
    pub(crate) fn lightyear_private_key(&self) -> [u8; PRIVATE_KEY_BYTES] {
        self.lypkey
    }
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    setup_logging();
    info!("Starting Edgegap Matchmaker");
    let bgnats = BevygapNats::new_and_connect("matchmaker").await?;
    let settings = Settings::parse();
    let lypkey = settings.parse_private_key();
    settings.validate_netcode_identity(lypkey);
    let api_config = if settings.mock_edgegap {
        info!(
            "Using mock Edgegap sessions at {}:{}",
            settings.mock_public_ip, settings.mock_external_port
        );
        Configuration::default()
    } else {
        edgegap_configuration(&settings)
    };
    let mm_state = MatchmakerState {
        nats: bgnats,
        api_config,
        settings,
        lypkey,
    };

    // ensure the specified app and version are valid and ready for players.
    if mm_state.settings.mock_edgegap {
        info!("Skipping Edgegap application verification in mock mode");
    } else {
        verify_application(&mm_state).await?;
    }

    let state = mm_state.clone();
    let _a = tokio::spawn(async move {
        session_request_streamer::streaming_session_request_handler(&state).await
    });

    let queue_session_deletes = !mm_state.settings.mock_edgegap;
    let state = mm_state.clone();
    let _cleanup =
        tokio::spawn(
            async move { session_cleanup_supervisor(&state, queue_session_deletes).await },
        );

    if mm_state.settings.mock_edgegap {
        info!("Skipping Edgegap API delete worker in mock mode");
    } else {
        let state = mm_state.clone();
        let _b = tokio::spawn(async move { delete_session_worker_supervisor(&state).await });
    }

    let state = mm_state.clone();
    let _watcher = tokio::spawn(async move {
        match watch_for_gameserver_announcements(&state).await {
            Ok(_) => info!("Gameserver announcement watcher completed"),
            Err(e) => error!("Error in gameserver announcement watcher: {}", e),
        }
    });

    let state = mm_state.clone();
    let session_service = tokio::spawn(async move {
        match session_request_supervisor(&state).await {
            Ok(_) => info!("Session service completed"),
            Err(e) => error!("Error in session service: {}", e),
        }
    });

    // just to block from exiting:
    if let Err(error) = session_service.await {
        error!("Session service task panicked or was cancelled: {error}");
    }

    // shouldn't get here
    info!("Edgegap Matchmaker exiting");
    Ok(())
    // dbg!(deployments);
}

async fn verify_application(state: &MatchmakerState) -> Result<(), async_nats::Error> {
    let config = state.configuration();
    let settings = &state.settings;

    let app = application_get(config, settings.app_name.as_str())
        .await
        .unwrap_or_else(|e| panic!("Edgegap API doesn't know this application name: {e}"));

    info!(
        "🟢 Application '{}', active: {}, last_updated: {}",
        app.name, app.is_active, app.last_updated
    );

    let app_version = app_version_get(
        config,
        settings.app_name.as_str(),
        settings.app_version.as_str(),
    )
    .await
    .unwrap_or_else(|e| panic!("Edgegap API doesn't know this application version: {e}"));

    if app_version.is_active.unwrap_or(false) {
        info!("🟢 Application version '{}' is active.", app_version.name);
    } else {
        error!(
            "🔴 Application version '{}' is not active, won't be able to create sessions.",
            app_version.name
        );
        // std::process::exit(1);
    }

    // info!("✅ {} @ {}", settings.app_name, settings.app_version);

    Ok(())
}

// https://fdeantoni.medium.com/from-env-logger-to-tokio-tracing-and-opentelemetry-adb247c0d40f
fn setup_logging() {
    // Set environment for logging configuration
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    // Start logging to console
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::Layer::default().compact())
        .init();
}
