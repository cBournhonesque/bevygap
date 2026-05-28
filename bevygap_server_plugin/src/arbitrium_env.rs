use bevy::prelude::*;

const LOCAL_CONTEXT_MODE_ENV: &str = "BEVYGAP_CONTEXT_MODE";
const LOCAL_CONTEXT_FLAG_ENV: &str = "BEVYGAP_LOCAL_CONTEXT";

/// Represents the environment variables provided by Arbitrium for deployments.
#[derive(Debug, Clone, Resource)]
pub struct ArbitriumEnv {
    /// Your deployment request ID. This is a unique ID across all Arbitrium. Can be used to retrieve information.
    pub request_id: String,
    /// URL to call to delete your deployment from within itself. Visit the API documentation for more details about this route.
    pub delete_url: String,
    /// Authorization token to call ARBITRIUM_DELETE_URL.
    pub delete_token: String,
    /// JSON encoded string that contains data about the location of your deployment.
    pub deployment_location: String,
    /// URL to get the context of your deployment. Visit the API documentation for more details about this route.
    pub context_url: String,
    /// Authorization token to call ARBITRIUM_CONTEXT_URL.
    pub context_token: String,
    /// The public IP of your deployment.
    pub public_ip: String,
    /// JSON string of the ports mapping of your deployment.
    pub ports_mapping: String,
    /// True when context should be synthesized locally instead of fetched from Edgegap.
    pub local_context: bool,
}

impl ArbitriumEnv {
    /// Creates a new instance of `ArbitriumEnv` from environment variables.
    pub fn from_env() -> Result<Self, std::env::VarError> {
        if local_context_enabled() {
            return Ok(Self::from_local_context_env());
        }

        Ok(Self {
            request_id: std::env::var("ARBITRIUM_REQUEST_ID")?,
            delete_url: std::env::var("ARBITRIUM_DELETE_URL")?,
            delete_token: std::env::var("ARBITRIUM_DELETE_TOKEN")?,
            deployment_location: std::env::var("ARBITRIUM_DEPLOYMENT_LOCATION")?,
            context_url: std::env::var("ARBITRIUM_CONTEXT_URL")?,
            context_token: std::env::var("ARBITRIUM_CONTEXT_TOKEN")?,
            public_ip: std::env::var("ARBITRIUM_PUBLIC_IP")?,
            ports_mapping: std::env::var("ARBITRIUM_PORTS_MAPPING")?,
            local_context: false,
        })
    }

    fn from_local_context_env() -> Self {
        let public_ip =
            std::env::var("ARBITRIUM_PUBLIC_IP").unwrap_or_else(|_| "127.0.0.1".to_string());
        let game_port = std::env::var("BEVYGAP_LOCAL_GAME_PORT")
            .or_else(|_| std::env::var("PORT"))
            .unwrap_or_else(|_| "7777".to_string());
        let ports_mapping = std::env::var("ARBITRIUM_PORTS_MAPPING").unwrap_or_else(|_| {
            format!(
                r#"{{"game":{{"name":"game","internal":{game_port},"external":{game_port},"protocol":"UDP"}}}}"#
            )
        });

        Self {
            request_id: std::env::var("ARBITRIUM_REQUEST_ID")
                .unwrap_or_else(|_| "local-lightrider".to_string()),
            delete_url: std::env::var("ARBITRIUM_DELETE_URL")
                .unwrap_or_else(|_| "local-mock://delete/local-lightrider".to_string()),
            delete_token: std::env::var("ARBITRIUM_DELETE_TOKEN")
                .unwrap_or_else(|_| "local-delete-token".to_string()),
            deployment_location: std::env::var("ARBITRIUM_DEPLOYMENT_LOCATION")
                .unwrap_or_else(|_| r#"{"city":"Local","country":"Dev"}"#.to_string()),
            context_url: std::env::var("ARBITRIUM_CONTEXT_URL")
                .unwrap_or_else(|_| "local-mock://context/local-lightrider".to_string()),
            context_token: std::env::var("ARBITRIUM_CONTEXT_TOKEN")
                .unwrap_or_else(|_| "local-context-token".to_string()),
            public_ip,
            ports_mapping,
            local_context: true,
        }
    }

    /// Returns a tuple containing the request_id and security_number extracted from the context_url.
    /// The security_number is parsed as an i32.
    pub fn context_parts(&self) -> Option<(String, i32)> {
        let parts: Vec<&str> = self.context_url.split('/').collect();
        if parts.len() >= 2 {
            let security_number = parts.last().and_then(|s| s.parse::<i32>().ok())?;
            let request_id = parts[parts.len() - 2].to_string();
            Some((request_id, security_number))
        } else {
            None
        }
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y" | "on" | "local" | "mock"
            )
        })
        .unwrap_or(false)
}

fn local_context_enabled() -> bool {
    env_flag(LOCAL_CONTEXT_FLAG_ENV)
        || std::env::var(LOCAL_CONTEXT_MODE_ENV)
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "local" | "mock" | "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
}
