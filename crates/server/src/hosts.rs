//! Host / Origin allowlist: blocks DNS rebinding and cross-site WebSockets.
//!
//! The server has no auth and defaults to loopback. A hostile page can resolve
//! its own name to 127.0.0.1 and then talk to `/api` same-origin; the browser
//! still sends the attacker's name in `Host`, so checking it closes that hole.
//! Only hostnames are compared (ports stripped), so the Vite dev proxy
//! (`Host: localhost:5173`) keeps working.

use std::net::SocketAddr;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone)]
pub struct HostAllowlist {
    hosts: Vec<String>,
}

impl HostAllowlist {
    pub fn new(bind: SocketAddr, extra: &[String]) -> Self {
        let mut hosts: Vec<String> = ["localhost", "127.0.0.1", "::1"]
            .into_iter()
            .map(String::from)
            .collect();
        if !bind.ip().is_unspecified() {
            hosts.push(bind.ip().to_string());
        }
        hosts.extend(extra.iter().map(|h| normalize(h)).filter(|h| !h.is_empty()));
        Self { hosts }
    }

    /// `Host` header value (`name`, `name:port`, `[v6]:port`).
    pub fn host_allowed(&self, host: &str) -> bool {
        let name = hostname(host);
        !name.is_empty() && self.hosts.contains(&name)
    }

    /// `Origin` header value (`scheme://host[:port]`).
    pub fn origin_allowed(&self, origin: &str) -> bool {
        let Some((scheme, rest)) = origin.split_once("://") else {
            return false;
        };
        if !matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https") {
            return false;
        }
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        self.host_allowed(authority)
    }
}

fn normalize(h: &str) -> String {
    h.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase()
}

/// Lowercased hostname with port and IPv6 brackets stripped.
fn hostname(authority: &str) -> String {
    let a = authority.trim();
    let name = if let Some(rest) = a.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else if a.matches(':').count() > 1 {
        // Bare IPv6 without brackets or port.
        a
    } else {
        a.split(':').next().unwrap_or("")
    };
    name.to_ascii_lowercase()
}

fn forbidden(path: &str, code: &str, message: &str) -> Response {
    if path == "/api" || path.starts_with("/api/") {
        let body = serde_json::json!({ "error": { "code": code, "message": message } });
        (StatusCode::FORBIDDEN, axum::Json(body)).into_response()
    } else {
        (StatusCode::FORBIDDEN, message.to_string()).into_response()
    }
}

/// Applied to the whole router, `/healthz` included (it is a loopback
/// service; probes behind a proxy should list their host in `--allowed-hosts`).
pub async fn guard(State(list): State<HostAllowlist>, req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let host_ok = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|h| list.host_allowed(h));
    if !host_ok {
        tracing::warn!(path, "rejected request: host not allowed");
        return forbidden(&path, "forbidden_host", "host not allowed");
    }
    if path == "/ws" {
        if let Some(origin) = req.headers().get(header::ORIGIN) {
            if !origin.to_str().is_ok_and(|o| list.origin_allowed(o)) {
                tracing::warn!(path, "rejected websocket: origin not allowed");
                return forbidden(&path, "forbidden_origin", "origin not allowed");
            }
        }
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(bind: &str, extra: &[&str]) -> HostAllowlist {
        let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
        HostAllowlist::new(bind.parse().unwrap(), &extra)
    }

    #[test]
    fn loopback_names_allowed_ports_stripped() {
        let l = list("127.0.0.1:8080", &[]);
        for h in [
            "localhost",
            "localhost:5173",
            "127.0.0.1:8080",
            "::1",
            "[::1]",
            "[::1]:8080",
            "LocalHost:80",
        ] {
            assert!(l.host_allowed(h), "{h}");
        }
    }

    #[test]
    fn foreign_and_empty_hosts_denied() {
        let l = list("127.0.0.1:8080", &[]);
        for h in [
            "evil.example",
            "evil.example:8080",
            "",
            ":8080",
            "10.0.0.5:8080",
            "localhost.evil.example",
        ] {
            assert!(!l.host_allowed(h), "{h}");
        }
    }

    #[test]
    fn bind_host_and_extras() {
        let l = list("192.168.1.10:8080", &["Box.Lan", " [fe80::1] "]);
        assert!(l.host_allowed("192.168.1.10:8080"));
        assert!(l.host_allowed("box.lan:8080"));
        assert!(l.host_allowed("[fe80::1]:8080"));
        // Unspecified bind adds nothing.
        let l = list("0.0.0.0:8080", &[]);
        assert!(!l.host_allowed("0.0.0.0:8080"));
        assert!(l.host_allowed("localhost"));
    }

    #[test]
    fn origin_parsing() {
        let l = list("127.0.0.1:8080", &[]);
        assert!(l.origin_allowed("http://localhost:5173"));
        assert!(l.origin_allowed("https://127.0.0.1:8080"));
        assert!(l.origin_allowed("http://[::1]:8080"));
        assert!(!l.origin_allowed("https://evil.example"));
        assert!(!l.origin_allowed("http://localhost.evil.example"));
        assert!(!l.origin_allowed("null"));
        assert!(!l.origin_allowed("garbage"));
        assert!(!l.origin_allowed("ftp://localhost"));
        assert!(!l.origin_allowed("http://"));
        assert!(!l.origin_allowed(""));
    }
}
