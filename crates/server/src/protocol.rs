//! Wire protocol types. One WebSocket, bidirectional.
//!
//! Client -> server: a JSON `start` handshake (Text), then raw binary PCM frames.
//! Server -> client: JSON Text frames only.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Control messages the client may send as Text frames.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMessage {
    Start(StartParams),
    /// Client is done speaking but wants the socket kept open for the final flush.
    Stop,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartParams {
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,
    #[serde(default = "default_encoding")]
    pub encoding: String,
    #[serde(default)]
    pub language: Option<String>,
    /// Resume this session instead of creating a new one.
    #[serde(default)]
    pub session_id: Option<Uuid>,
}

impl Default for StartParams {
    fn default() -> Self {
        Self {
            sample_rate: default_sample_rate(),
            encoding: default_encoding(),
            language: None,
            session_id: None,
        }
    }
}

impl StartParams {
    pub fn language_or_default(&self) -> String {
        self.language.clone().unwrap_or_else(|| "en".to_string())
    }
}

fn default_sample_rate() -> u32 {
    16_000
}

fn default_encoding() -> String {
    "pcm_s16le".to_string()
}

/// A newly committed (stable) chunk of transcript. Append-only delta.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub text: String,
    /// Absolute ms from stream start.
    pub start: u64,
    /// Absolute ms from stream start.
    pub end: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ServerMessage {
    /// Handshake accepted, model warm, start sending audio.
    Ready {
        session_id: Uuid,
        /// True when this connection continues an existing session.
        resumed: bool,
        /// Where this connection starts on the session timeline. Wire
        /// timestamps stay relative to this connection's stream.
        offset_ms: i64,
    },
    Transcript {
        seq: u64,
        /// Omitted on ticks that produced no new committed text.
        #[serde(skip_serializing_if = "Option::is_none")]
        commit: Option<Commit>,
        /// Unstable hypothesis tail. REPLACE semantics, always present.
        pending: String,
        end_of_segment: bool,
        language: String,
    },
    Error {
        code: String,
        message: String,
    },
    /// Stream finished; `text` is the full committed transcript.
    Done {
        text: String,
    },
}

impl ServerMessage {
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        ServerMessage::Error {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_start_handshake() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type":"start","sampleRate":48000,"encoding":"pcm_s16le","language":"en"}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Start(p) => {
                assert_eq!(p.sample_rate, 48_000);
                assert_eq!(p.language.as_deref(), Some("en"));
            }
            _ => panic!("expected start"),
        }
    }

    #[test]
    fn transcript_omits_absent_commit() {
        let json = serde_json::to_string(&ServerMessage::Transcript {
            seq: 3,
            commit: None,
            pending: "hello wor".into(),
            end_of_segment: false,
            language: "en".into(),
        })
        .unwrap();
        assert!(!json.contains("commit"));
        assert!(json.contains(r#""endOfSegment":false"#));
    }

    #[test]
    fn start_accepts_optional_session_id() {
        let with: ClientMessage = serde_json::from_str(
            r#"{"type":"start","sessionId":"0192f0c1-0000-7000-8000-000000000001"}"#,
        )
        .unwrap();
        let ClientMessage::Start(p) = with else {
            panic!("expected start")
        };
        assert_eq!(
            p.session_id.map(|u| u.to_string()).as_deref(),
            Some("0192f0c1-0000-7000-8000-000000000001")
        );

        let without: ClientMessage = serde_json::from_str(r#"{"type":"start"}"#).unwrap();
        let ClientMessage::Start(p) = without else {
            panic!("expected start")
        };
        assert!(p.session_id.is_none());
    }

    #[test]
    fn rejects_malformed_session_id() {
        assert!(
            serde_json::from_str::<ClientMessage>(r#"{"type":"start","sessionId":"nope"}"#)
                .is_err()
        );
    }

    #[test]
    fn ready_carries_session_fields() {
        let id: uuid::Uuid = "0192f0c1-0000-7000-8000-000000000001".parse().unwrap();
        let json = serde_json::to_string(&ServerMessage::Ready {
            session_id: id,
            resumed: true,
            offset_ms: 1_500,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"ready","sessionId":"0192f0c1-0000-7000-8000-000000000001","resumed":true,"offsetMs":1500}"#
        );
    }
}
