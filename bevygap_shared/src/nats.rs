use async_nats::jetstream::stream::Stream;
use async_nats::jetstream::{self, stream};
use async_nats::Client;
use std::time::Duration;

use log::*;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "bevy", derive(bevy::prelude::Resource))]
pub struct BevygapNats {
    client: Client,
    kv_s2c: jetstream::kv::Store,
    kv_c2s: jetstream::kv::Store,
    kv_cert_digests: jetstream::kv::Store,
    kv_active_connections: jetstream::kv::Store,
    kv_deployment_metrics: jetstream::kv::Store,
    kv_unclaimed_sessions: jetstream::kv::Store,
    delete_session_stream: Stream,
    delete_session_subject_prefix: String,
}

const NATS_NAMESPACE_ENV: &str = "BEVYGAP_NATS_NAMESPACE";
const SESSION_MAPPING_TTL_MS_ENV: &str = "BEVYGAP_SESSION_MAPPING_TTL_MS";
const UNCLAIMED_SESSION_TTL_SECS_ENV: &str = "BEVYGAP_UNCLAIMED_SESSION_TTL_SECS";
const ACTIVE_CONNECTION_TTL_SECS_ENV: &str = "BEVYGAP_ACTIVE_CONNECTION_TTL_SECS";
const CERT_DIGEST_TTL_SECS_ENV: &str = "BEVYGAP_CERT_DIGEST_TTL_SECS";
const DEPLOYMENT_METRICS_TTL_SECS_ENV: &str = "BEVYGAP_DEPLOYMENT_METRICS_TTL_SECS";

const DELETE_SESSION_SUBJECT_BASE: &str = "edgegap_delete_session_q";
const DELETE_SESSION_STREAM_BASE: &str = "DELETE_SESSION_STREAM";
const MATCHMAKER_REQUEST_SUBJECT_BASE: &str = "matchmaker.request";

fn sanitize_kv_token(value: &str) -> String {
    let token = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if token.is_empty() {
        "unknown".to_string()
    } else {
        token
    }
}

pub fn nats_namespace() -> Option<String> {
    std::env::var(NATS_NAMESPACE_ENV)
        .ok()
        .map(|value| sanitize_kv_token(value.trim()))
        .filter(|value| value != "unknown")
}

pub fn nats_bucket_name(base: &str) -> String {
    match nats_namespace() {
        Some(namespace) => format!("{namespace}_{base}"),
        None => base.to_string(),
    }
}

pub fn nats_stream_name(base: &str) -> String {
    match nats_namespace() {
        Some(namespace) => format!("{namespace}_{base}"),
        None => base.to_string(),
    }
}

pub fn nats_subject_name(base: &str) -> String {
    match nats_namespace() {
        Some(namespace) => format!("{namespace}.{base}"),
        None => base.to_string(),
    }
}

pub fn matchmaker_request_subject(game_name: &str, game_version: &str) -> String {
    nats_subject_name(&format!(
        "{MATCHMAKER_REQUEST_SUBJECT_BASE}.{}.{}",
        sanitize_kv_token(game_name),
        sanitize_kv_token(game_version)
    ))
}

fn delete_session_subject_prefix() -> String {
    nats_subject_name(DELETE_SESSION_SUBJECT_BASE)
}

pub fn cert_digest_deployment_key(request_id: &str) -> String {
    format!("deployment.{}", sanitize_kv_token(request_id))
}

pub fn cert_digest_endpoint_key(public_ip: &str, external_port: u16) -> String {
    format!(
        "endpoint.{}.{}",
        sanitize_kv_token(public_ip),
        external_port
    )
}

pub fn cert_digest_public_ip_key(public_ip: &str) -> String {
    format!("ip.{}", sanitize_kv_token(public_ip))
}

pub fn deployment_metrics_key(request_id: &str) -> String {
    format!("deployment.{}", sanitize_kv_token(request_id))
}

/// Ordered keys used to publish and resolve WebTransport certificate digests.
///
/// Deployment request id is preferred because it is globally unique. Endpoint
/// is the practical fallback for local/mock flows and for Edgegap responses
/// where a deployment id is not available to the matchmaker yet. The raw public
/// IP is kept last for compatibility with older Bevygap servers.
pub fn cert_digest_lookup_keys(
    request_id: Option<&str>,
    public_ip: &str,
    external_port: Option<u16>,
) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(request_id) = request_id.filter(|value| !value.trim().is_empty()) {
        keys.push(cert_digest_deployment_key(request_id));
    }
    if let Some(external_port) = external_port {
        keys.push(cert_digest_endpoint_key(public_ip, external_port));
    }
    keys.push(cert_digest_public_ip_key(public_ip));
    if !keys.iter().any(|key| key == public_ip) {
        keys.push(public_ip.to_string());
    }
    keys
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y" | "on"
            )
        })
        .unwrap_or(false)
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    match std::env::var(name) {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(error) => {
                warn!("Invalid {name}={value:?}; using {default_ms}ms: {error}");
                Duration::from_millis(default_ms)
            }
        },
        Err(_) => Duration::from_millis(default_ms),
    }
}

fn env_duration_secs(name: &str, default_secs: u64) -> Duration {
    match std::env::var(name) {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(secs) => Duration::from_secs(secs),
            Err(error) => {
                warn!("Invalid {name}={value:?}; using {default_secs}s: {error}");
                Duration::from_secs(default_secs)
            }
        },
        Err(_) => Duration::from_secs(default_secs),
    }
}

impl BevygapNats {
    /// Connects to NATS based on environment variables.
    pub async fn new_and_connect(nats_client_name: &str) -> Result<Self, async_nats::Error> {
        let client = Self::connect_to_nats(nats_client_name).await?;
        if let Some(namespace) = nats_namespace() {
            info!("NATS: using Bevygap namespace '{namespace}'");
        } else {
            info!("NATS: using default un-namespaced Bevygap subjects and buckets");
        }
        let (kv_s2c, kv_c2s) = Self::create_kv_buckets_for_session_mappings(client.clone()).await?;
        let kv_active_connections = Self::create_kv_active_connections(client.clone()).await?;
        let kv_cert_digests = Self::create_kv_cert_digests(client.clone()).await?;
        let kv_deployment_metrics = Self::create_kv_deployment_metrics(client.clone()).await?;
        let kv_unclaimed_sessions = Self::create_kv_unclaimed_sessions(client.clone()).await?;
        let delete_session_stream = Self::create_session_delete_queue(&client).await?;
        let delete_session_subject_prefix = delete_session_subject_prefix();
        Ok(Self {
            client,
            kv_s2c,
            kv_c2s,
            kv_cert_digests,
            kv_active_connections,
            kv_deployment_metrics,
            kv_unclaimed_sessions,
            delete_session_stream,
            delete_session_subject_prefix,
        })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn kv_s2c(&self) -> &jetstream::kv::Store {
        &self.kv_s2c
    }
    pub fn kv_c2s(&self) -> &jetstream::kv::Store {
        &self.kv_c2s
    }
    pub fn kv_active_connections(&self) -> &jetstream::kv::Store {
        &self.kv_active_connections
    }
    pub fn kv_unclaimed_sessions(&self) -> &jetstream::kv::Store {
        &self.kv_unclaimed_sessions
    }
    pub fn kv_cert_digests(&self) -> &jetstream::kv::Store {
        &self.kv_cert_digests
    }
    pub fn kv_deployment_metrics(&self) -> &jetstream::kv::Store {
        &self.kv_deployment_metrics
    }
    pub fn delete_session_stream(&self) -> &Stream {
        &self.delete_session_stream
    }

    /// Enqueues a job to delete a session id via the edgegap API
    pub async fn enqueue_session_delete(
        &self,
        session_id: String,
    ) -> Result<(), async_nats::Error> {
        let js = jetstream::new(self.client.clone());
        js.publish(
            format!(
                "{}.{}",
                self.delete_session_subject_prefix,
                sanitize_kv_token(&session_id)
            ),
            session_id.into(),
        )
        .await?
        .await?;
        Ok(())
    }

    /// want to support multiple connection modes. In production, I have a domain name with
    /// LetsEncrypt certs set up, so i just need to enable TLS and provide user/pass.
    ///
    /// In other scenarios I want to support self-signed certs, in which case we need to provide
    /// the CA, so our NATS client can verify the server.
    ///
    /// TLS is assumed, and by default we expect a trusted (LetsEncrypt or similar) cert.
    /// if the NATS_CA=/path/to/ca.pem env is set, we use that CA to verify the cert.
    ///
    /// If NATS_CA_CONTENTS is set, we write it to a temp file and use that as the CA.
    ///
    /// Setting NATS_INSECURE to a truthy value disables TLS entirely (still need user/pass).
    /// In production, set BEVYGAP_REQUIRE_SECURE_NATS=1 to reject insecure NATS and default
    /// development credentials at startup.
    async fn connect_to_nats(nats_client_name: &str) -> Result<Client, async_nats::Error> {
        info!("NATS: setting up, client name: {nats_client_name}");

        let nats_insecure = env_flag("NATS_INSECURE");
        let require_secure_nats = env_flag("BEVYGAP_REQUIRE_SECURE_NATS");
        let allow_dev_credentials = env_flag("BEVYGAP_ALLOW_DEV_NATS_CREDENTIALS");
        let nats_self_signed_ca: Option<String> = std::env::var("NATS_CA").ok().or_else(|| {
            // we write out the CA to a temp file, if provided in NATS_CA_CONTENTS
            // this is useful for deploying containers on edgegap and injecting CA root certs.
            //
            // However, as of 5 November 2024, Edgegap limits you to 255 bytes in ENV vars
            // so this is actually set by the server, from a command line arg.. see the book!
            if let Ok(ca_contents) = std::env::var("NATS_CA_CONTENTS") {
                let sanitised_nats_client_name = nats_client_name
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .collect::<String>();
                let tmp_file =
                    std::env::temp_dir().join(format!("rootCA-{sanitised_nats_client_name}.pem"));
                match std::fs::write(&tmp_file, ca_contents) {
                    Ok(()) => Some(tmp_file.to_string_lossy().to_string()),
                    Err(error) => {
                        error!("Failed to write NATS_CA_CONTENTS to {tmp_file:?}: {error}");
                        None
                    }
                }
            } else {
                None
            }
        });

        let nats_host = std::env::var("NATS_HOST").expect("Missing NATS_HOST env");
        let nats_user = std::env::var("NATS_USER").expect("Missing NATS_USER env");
        let nats_pass = std::env::var("NATS_PASSWORD").expect("Missing NATS_PASSWORD env");

        if require_secure_nats && nats_insecure {
            panic!(
                "BEVYGAP_REQUIRE_SECURE_NATS=1 forbids NATS_INSECURE; configure NATS TLS or remove the production secure-NATS requirement"
            );
        }
        if require_secure_nats
            && !allow_dev_credentials
            && nats_user == "lightrider"
            && nats_pass == "lightrider"
        {
            panic!(
                "BEVYGAP_REQUIRE_SECURE_NATS=1 forbids default NATS credentials; set strong NATS_USER/NATS_PASSWORD or BEVYGAP_ALLOW_DEV_NATS_CREDENTIALS=1 for local-only testing"
            );
        }

        if nats_insecure {
            warn!("😬 NATS: insecure - TLS is disabled.");
        } else {
            info!("NATS: TLS is enabled");
        }
        if require_secure_nats {
            info!("NATS: production security checks are enabled");
        }

        info!("NATS: connecting as '{nats_user}' to {nats_host}");

        let mut nats_connection = async_nats::ConnectOptions::new()
            .name(nats_client_name)
            .user_and_password(nats_user, nats_pass)
            .max_reconnects(10)
            .require_tls(!nats_insecure);

        if let Some(ca) = nats_self_signed_ca {
            info!("NATS: using self-signed CA: {}", ca);
            nats_connection = nats_connection.add_root_certificates(ca.into());
        } else {
            info!("NATS: expecting a trusted cert, no self-signed CA provided.");
        }

        let client = nats_connection.connect(nats_host).await?;
        info!("🟢 NATS: connected OK");
        Ok(client)

        // if let Some(ca) = nats_self_signed_ca {
        //     info!("NATS_SELF_SIGNED_CA: {}", ca);
        //     let nats_ca = std::env::var("NATS_CA").unwrap_or("./config/rootCA.pem".to_string());
        // }
        // let nats_cert =
        // std::env::var("NATS_CERT").unwrap_or("./config/client-cert.pem".to_string());
        // let nats_key = std::env::var("NATS_KEY").unwrap_or("./config/client-key.pem".to_string());

        // info!("NATS_CA: {}", nats_ca);
        // info!("NATS_CERT: {}", nats_cert);
        // info!("NATS_KEY: {}", nats_key);

        // let client = async_nats::ConnectOptions::new()
        //     .name(nats_client_name)
        //     .user_and_password(nats_user, nats_pass)
        //     .max_reconnects(10)
        //     .require_tls(!insecure)
        //     // .add_root_certificates(nats_ca.into())
        //     // .add_client_certificate(nats_cert.into(), nats_key.into())
        //     .connect(nats_host)
        //     .await?;
    }

    pub async fn create_kv_active_connections(
        client: Client,
    ) -> Result<jetstream::kv::Store, async_nats::Error> {
        let jetstream = jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("active_connections"),
                max_age: env_duration_secs(ACTIVE_CONNECTION_TTL_SECS_ENV, 86400),
                ..Default::default()
            })
            .await?;
        Ok(kv)
    }

    pub async fn create_kv_unclaimed_sessions(
        client: Client,
    ) -> Result<jetstream::kv::Store, async_nats::Error> {
        let jetstream = jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("unclaimed_sessions"),
                max_value_size: 1024,
                description: "Any session ids we get from the API are stored here, and if they key age gets too big, we delete the session via the API.".to_string(),
                max_age: env_duration_secs(UNCLAIMED_SESSION_TTL_SECS_ENV, 180),
                ..Default::default()
            })
            .await?;
        Ok(kv)
    }

    pub async fn create_session_delete_queue(client: &Client) -> Result<Stream, async_nats::Error> {
        let js = jetstream::new(client.clone());
        let subject_prefix = delete_session_subject_prefix();
        let stream = js
            .create_stream(jetstream::stream::Config {
                name: nats_stream_name(DELETE_SESSION_STREAM_BASE),
                retention: stream::RetentionPolicy::WorkQueue,
                subjects: vec![format!("{subject_prefix}.*")],
                ..Default::default()
            })
            .await?;
        Ok(stream)
    }

    pub async fn create_kv_cert_digests(
        client: Client,
    ) -> Result<jetstream::kv::Store, async_nats::Error> {
        let jetstream = jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("cert_digests"),
                description: "Maps server deployment/endpoint keys to self-signed cert digests"
                    .to_string(),
                max_age: env_duration_secs(CERT_DIGEST_TTL_SECS_ENV, 86400 * 14),
                max_value_size: 1024,

                ..Default::default()
            })
            .await?;
        Ok(kv)
    }

    pub async fn create_kv_deployment_metrics(
        client: Client,
    ) -> Result<jetstream::kv::Store, async_nats::Error> {
        let jetstream = jetstream::new(client);
        let kv = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("deployment_metrics"),
                description:
                    "Game-server room and deployment capacity heartbeats used by matchmaker"
                        .to_string(),
                max_age: env_duration_secs(DEPLOYMENT_METRICS_TTL_SECS_ENV, 30),
                max_value_size: 16 * 1024,
                ..Default::default()
            })
            .await?;
        Ok(kv)
    }

    /// Creates two buckets for mapping between LY client ids and Edgegap session tokens
    async fn create_kv_buckets_for_session_mappings(
        client: Client,
    ) -> Result<(jetstream::kv::Store, jetstream::kv::Store), async_nats::Error> {
        let jetstream = jetstream::new(client);

        let kv_s2c = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("sessions_eg2ly"),
                description: "Maps Edgegap Session IDs to Lightyear Client IDs".to_string(),
                max_value_size: 1024,
                // shouldn't need long for the client to receive token, and make connection to gameserver.
                max_age: env_duration_ms(SESSION_MAPPING_TTL_MS_ENV, 30000),
                // storage: StorageType::File,
                ..Default::default()
            })
            .await?;

        let kv_c2s = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: nats_bucket_name("sessions_ly2eg"),
                description: "Maps Lightyear Client IDs to Edgegap Session IDs".to_string(),
                max_value_size: 1024,
                // shouldn't need long for the client to receive token, and make connection to gameserver.
                max_age: env_duration_ms(SESSION_MAPPING_TTL_MS_ENV, 30000),
                // storage: StorageType::File,
                ..Default::default()
            })
            .await?;

        Ok((kv_s2c, kv_c2s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn cert_digest_keys_prefer_deployment_then_endpoint_then_legacy_ip() {
        assert_eq!(
            cert_digest_lookup_keys(Some("deploy-123"), "127.0.0.1", Some(7777)),
            vec![
                "deployment.deploy-123",
                "endpoint.127_0_0_1.7777",
                "ip.127_0_0_1",
                "127.0.0.1"
            ]
        );
    }

    #[test]
    fn cert_digest_keys_work_without_deployment_id() {
        assert_eq!(
            cert_digest_lookup_keys(None, "2001:db8::1", Some(31302)),
            vec![
                "endpoint.2001_db8__1.31302",
                "ip.2001_db8__1",
                "2001:db8::1"
            ]
        );
    }

    #[test]
    fn nats_names_are_default_compatible_without_namespace() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var(NATS_NAMESPACE_ENV);

        assert_eq!(nats_bucket_name("sessions_ly2eg"), "sessions_ly2eg");
        assert_eq!(
            nats_stream_name("DELETE_SESSION_STREAM"),
            "DELETE_SESSION_STREAM"
        );
        assert_eq!(
            matchmaker_request_subject("lightrider", "dev"),
            "matchmaker.request.lightrider.dev"
        );
        assert_eq!(
            deployment_metrics_key("deploy/123"),
            "deployment.deploy_123"
        );
    }

    #[test]
    fn nats_names_include_sanitized_namespace_when_configured() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var(NATS_NAMESPACE_ENV, "lightrider/dev");

        assert_eq!(
            nats_bucket_name("sessions_ly2eg"),
            "lightrider_dev_sessions_ly2eg"
        );
        assert_eq!(
            nats_stream_name("DELETE_SESSION_STREAM"),
            "lightrider_dev_DELETE_SESSION_STREAM"
        );
        assert_eq!(
            matchmaker_request_subject("light rider", "v0.0.1"),
            "lightrider_dev.matchmaker.request.light_rider.v0_0_1"
        );

        std::env::remove_var(NATS_NAMESPACE_ENV);
    }
}
