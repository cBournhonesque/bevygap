use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Debug)]
pub enum SessionRequestFeedback {
    /// The service has begun processing the request.
    Acknowledged,
    /// The edgegap session was created, we are now awaiting readyness
    SessionRequestAccepted(String),
    /// Session readyness update
    ProgressReport(String),
    /// The session is ready to connect to
    SessionReady {
        token: String,
        ip: String,
        port: u16,
        cert_digest: String,
    },
    /// There was an error.
    Error(u16, String),
}

impl fmt::Display for SessionRequestFeedback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionRequestFeedback::Acknowledged => write!(f, "Sending request"),
            SessionRequestFeedback::SessionRequestAccepted(id) => {
                write!(f, "Request accepted: {}", id)
            }
            SessionRequestFeedback::ProgressReport(msg) => write!(f, "In-progress: {msg}"),
            SessionRequestFeedback::SessionReady {
                token: _,
                ip,
                port,
                cert_digest: _,
            } => write!(f, "Session Ready! {ip}:{port}"),
            SessionRequestFeedback::Error(code, msg) => write!(f, "Error {code}: {msg}"),
        }
    }
}

/// Send up the websocket to the matchmaker when a client wants to play.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestSession {
    /// name of game to play
    pub game: String,
    /// version of game to play
    pub version: String,
    /// client ip address override
    pub client_ip: Option<String>,
    /// Optional game-specific room intent. The matchmaker treats this as
    /// capacity/routing data; the game server remains authoritative for the
    /// final room join after connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomSelection>,
}

impl RequestSession {
    pub fn game_name_and_version(&self) -> Result<(String, String), String> {
        let name_pattern = regex::Regex::new(r"^[a-zA-Z0-9\s_-]+$").unwrap();
        let ver_pattern = regex::Regex::new(r"^[a-zA-Z0-9\s_-]+$").unwrap();

        if !name_pattern.is_match(&self.game) {
            return Err("Game name invalid".to_string());
        }

        if !ver_pattern.is_match(&self.version) {
            return Err("Game version invalid".to_string());
        }

        if self.game.len() > 30 {
            return Err("Game name too long (max 30 chars)".to_string());
        }

        if self.version.len() > 30 {
            return Err("Game version too long (max 30 chars)".to_string());
        }

        Ok((self.game.clone(), self.version.clone()))
    }

    pub fn room_selection(&self) -> RoomSelection {
        self.room.clone().unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(tag = "mode", content = "value", rename_all = "snake_case")]
pub enum RoomSelection {
    #[default]
    Auto,
    New,
    Code(String),
    Id(String),
}

impl RoomSelection {
    pub fn room_key(&self) -> Option<String> {
        match self {
            RoomSelection::Auto | RoomSelection::New => None,
            RoomSelection::Code(code) => Some(format!("code:{}", normalize_room_token(code))),
            RoomSelection::Id(id) => Some(format!("id:{}", normalize_room_token(id))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DeploymentRoomMetrics {
    pub key: String,
    pub private: bool,
    pub players: u32,
    pub max_players: u32,
}

impl DeploymentRoomMetrics {
    pub fn has_player_capacity(&self) -> bool {
        self.players < self.max_players.max(1)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DeploymentMetrics {
    pub request_id: String,
    pub public_ip: String,
    pub external_port: Option<u16>,
    pub total_players: u32,
    pub max_players: u32,
    pub max_rooms: u32,
    pub cpu_percent: Option<f32>,
    pub rooms: Vec<DeploymentRoomMetrics>,
}

impl DeploymentMetrics {
    pub fn room_count(&self) -> u32 {
        self.rooms.len() as u32
    }

    pub fn has_deployment_player_capacity(&self, policy_max_players: u32) -> bool {
        self.total_players < self.max_players.max(1).min(policy_max_players.max(1))
    }

    pub fn has_room_capacity(&self, policy_max_rooms: u32) -> bool {
        self.room_count() < self.max_rooms.max(1).min(policy_max_rooms.max(1))
    }
}

fn normalize_room_token(value: &str) -> String {
    let normalized = value
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized.to_ascii_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_session_defaults_to_auto_room() {
        let request = RequestSession {
            game: "game".to_string(),
            version: "dev".to_string(),
            client_ip: None,
            room: None,
        };
        assert_eq!(request.room_selection(), RoomSelection::Auto);
    }

    #[test]
    fn room_selection_keys_are_stable() {
        assert_eq!(
            RoomSelection::Code("ab-c".to_string())
                .room_key()
                .as_deref(),
            Some("code:AB-C")
        );
        assert_eq!(
            RoomSelection::Id("42".to_string()).room_key().as_deref(),
            Some("id:42")
        );
        assert_eq!(RoomSelection::Auto.room_key(), None);
    }
}
