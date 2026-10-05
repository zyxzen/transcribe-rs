//! REST API over persisted sessions: browse, rename, delete, export.

use std::path::PathBuf;

use axum::body::{Body, Bytes};
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::db;
use crate::export::{self, Paragraph};
use crate::live::LiveRegistry;
use crate::sessions;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;
const MAX_TITLE_CHARS: usize = 200;

#[derive(Clone)]
pub struct ApiState {
    pub db: PgPool,
    pub live: LiveRegistry,
    pub data_dir: PathBuf,
}

pub fn router<S>(state: ApiState) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/sessions", get(list_sessions))
        .route(
            "/sessions/{id}",
            get(get_session)
                .patch(rename_session)
                .delete(delete_session),
        )
        .route("/sessions/{id}/export/{format}", get(export_session))
        .fallback(|| async { ApiError::NotFound })
        .with_state(state)
}

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    BadRequest(String),
    Conflict(&'static str, &'static str),
    Unprocessable(&'static str, String),
    Internal(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError::Internal(e.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not_found", "not found".to_string()),
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            ApiError::Conflict(code, message) => (StatusCode::CONFLICT, code, message.to_string()),
            ApiError::Unprocessable(code, message) => {
                (StatusCode::UNPROCESSABLE_ENTITY, code, message)
            }
            ApiError::Internal(e) => {
                tracing::error!(error = ?e, "api request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "internal error".to_string(),
                )
            }
        };
        (
            status,
            Json(serde_json::json!({ "error": { "code": code, "message": message } })),
        )
            .into_response()
    }
}

/// Extractor rejections carry axum's own text; surface it in the JSON contract.
fn bad_request(e: impl std::fmt::Display) -> ApiError {
    ApiError::BadRequest(e.to_string())
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(Serialize)]
struct SessionSummary {
    #[serde(flatten)]
    item: db::SessionListItem,
    live: bool,
}

#[derive(Serialize)]
struct SessionView {
    #[serde(flatten)]
    session: db::Session,
    live: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ParagraphView {
    index: i32,
    start_ms: i64,
    end_ms: i64,
    text: String,
}

#[derive(Serialize)]
struct SessionDetail {
    #[serde(flatten)]
    session: SessionView,
    recordings: Vec<db::Recording>,
    paragraphs: Vec<ParagraphView>,
}

#[derive(Serialize)]
struct ExportDocument<'a> {
    session: &'a SessionView,
    recordings: &'a [db::Recording],
    paragraphs: &'a [Paragraph],
}

#[derive(Deserialize)]
struct ListParams {
    limit: Option<i64>,
    before: Option<Uuid>,
}

#[derive(Deserialize)]
struct RenameBody {
    title: Option<String>,
}

async fn list_sessions(
    State(st): State<ApiState>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> ApiResult<Json<Vec<SessionSummary>>> {
    let Query(params) = params.map_err(bad_request)?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let items = db::list_sessions(&st.db, params.before, limit).await?;
    Ok(Json(
        items
            .into_iter()
            .map(|item| SessionSummary {
                live: st.live.is_live(item.id),
                item,
            })
            .collect(),
    ))
}

async fn load(st: &ApiState, id: Uuid) -> ApiResult<SessionView> {
    let session = db::get_session(&st.db, id)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(SessionView {
        live: st.live.is_live(id),
        session,
    })
}

async fn get_session(
    State(st): State<ApiState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> ApiResult<Json<SessionDetail>> {
    let Path(id) = id.map_err(bad_request)?;
    let session = load(&st, id).await?;
    let recordings = db::list_recordings(&st.db, id).await?;
    let paragraphs = export::paragraphs(&db::list_chunks(&st.db, id).await?)
        .into_iter()
        .map(|p| ParagraphView {
            index: p.index,
            start_ms: p.start_ms,
            end_ms: p.end_ms,
            text: p.text,
        })
        .collect();
    Ok(Json(SessionDetail {
        session,
        recordings,
        paragraphs,
    }))
}

async fn rename_session(
    State(st): State<ApiState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<RenameBody>, JsonRejection>,
) -> ApiResult<Json<SessionView>> {
    let Path(id) = id.map_err(bad_request)?;
    let Json(body) = body.map_err(bad_request)?;
    let title = body
        .title
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty());
    if title.is_some_and(|t| t.chars().count() > MAX_TITLE_CHARS) {
        return Err(ApiError::Unprocessable(
            "title_too_long",
            format!("titles are limited to {MAX_TITLE_CHARS} characters"),
        ));
    }
    let session = db::update_title(&st.db, id, title)
        .await?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(SessionView {
        live: st.live.is_live(id),
        session,
    }))
}

async fn delete_session(
    State(st): State<ApiState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> ApiResult<StatusCode> {
    let Path(id) = id.map_err(bad_request)?;
    // Holding the claim while deleting stops a reconnect from resuming a
    // session that is halfway gone.
    let Some(_guard) = st.live.try_claim(id) else {
        return Err(ApiError::Conflict(
            "session_live",
            "stop recording before deleting this session",
        ));
    };
    if !db::delete_session(&st.db, id).await? {
        return Err(ApiError::NotFound);
    }
    sessions::delete_files(&st.data_dir, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn export_session(
    State(st): State<ApiState>,
    path: Result<Path<(Uuid, String)>, PathRejection>,
) -> ApiResult<Response> {
    let Path((id, format)) = path.map_err(bad_request)?;
    if !matches!(format.as_str(), "txt" | "srt" | "vtt" | "json" | "wav") {
        return Err(ApiError::NotFound);
    }
    // Repair only when nobody is recording; a live recording's missing
    // duration is not damage.
    if let Some(_guard) = st.live.try_claim(id) {
        sessions::repair(&st.db, &st.data_dir, id).await?;
    }
    let session = load(&st, id).await?;
    let filename = export::filename(
        session.session.title.as_deref(),
        session.session.created_at,
        &format,
    );

    let (content_type, body) = if format == "wav" {
        ("audio/wav", wav_body(&st, id).await?)
    } else {
        let paragraphs = export::paragraphs(&db::list_chunks(&st.db, id).await?);
        match format.as_str() {
            "txt" => (
                "text/plain; charset=utf-8",
                Body::from(export::to_txt(&paragraphs)),
            ),
            "srt" => (
                "application/x-subrip; charset=utf-8",
                Body::from(export::to_srt(&export::build_cues(&paragraphs))),
            ),
            "vtt" => (
                "text/vtt; charset=utf-8",
                Body::from(export::to_vtt(&export::build_cues(&paragraphs))),
            ),
            _ => {
                let recordings = db::list_recordings(&st.db, id).await?;
                let doc = ExportDocument {
                    session: &session,
                    recordings: &recordings,
                    paragraphs: &paragraphs,
                };
                (
                    "application/json",
                    Body::from(serde_json::to_vec_pretty(&doc)?),
                )
            }
        }
    };

    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        body,
    )
        .into_response())
}

/// Header plus every recording's PCM, streamed in order without buffering
/// the session in memory. Lengths are fixed up front so a live recording
/// growing during the download cannot desync the header.
async fn wav_body(st: &ApiState, id: Uuid) -> ApiResult<Body> {
    let recordings = db::list_recordings(&st.db, id).await?;
    let sample_rate = recordings.first().map_or(16_000, |r| r.sample_rate);
    if recordings.iter().any(|r| r.sample_rate != sample_rate) {
        return Err(ApiError::Unprocessable(
            "mixed_sample_rates",
            "recordings in this session use different sample rates".into(),
        ));
    }

    let mut parts: Vec<BoxStream<'static, std::io::Result<Bytes>>> = Vec::new();
    let mut total: u64 = 0;
    for rec in &recordings {
        let path = sessions::recording_path(&st.data_dir, id, rec.id);
        let len = match tokio::fs::metadata(&path).await {
            // Even length: a live recording may be mid-sample.
            Ok(meta) => meta.len() & !1,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "recording audio missing from export");
                continue;
            }
        };
        if len == 0 {
            continue;
        }
        let file = tokio::fs::File::open(&path).await?;
        total += len;
        parts.push(ReaderStream::new(file.take(len)).boxed());
    }

    let header = Bytes::copy_from_slice(&export::wav_header(
        sample_rate as u32,
        u32::try_from(total).unwrap_or(u32::MAX),
    ));
    let body = stream::once(async move { Ok::<_, std::io::Error>(header) })
        .chain(stream::iter(parts).flatten());
    Ok(Body::from_stream(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{HeaderMap, Request};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::path::Path as FsPath;
    use tower::ServiceExt;

    struct Fixture {
        app: Router,
        pool: PgPool,
        live: LiveRegistry,
        dir: tempfile::TempDir,
    }

    fn fixture(pool: PgPool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let live = LiveRegistry::default();
        let app = router(ApiState {
            db: pool.clone(),
            live: live.clone(),
            data_dir: dir.path().into(),
        });
        Fixture {
            app,
            pool,
            live,
            dir,
        }
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut req = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, headers, bytes)
    }

    fn json(bytes: &Bytes) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    /// Session with one closed 3 s recording and two paragraphs.
    async fn seed(pool: &PgPool, dir: &FsPath, title: Option<&str>) -> Uuid {
        let sid = Uuid::now_v7();
        db::create_session(pool, sid, "en").await.unwrap();
        if let Some(t) = title {
            db::update_title(pool, sid, Some(t)).await.unwrap();
        }
        let r = db::begin_recording(pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        db::insert_chunks(
            pool,
            sid,
            r.recording.id,
            &[
                db::Chunk {
                    paragraph: 0,
                    text: "hello there".into(),
                    start_ms: 0,
                    end_ms: 900,
                },
                db::Chunk {
                    paragraph: 0,
                    text: "general".into(),
                    start_ms: 900,
                    end_ms: 1_500,
                },
                db::Chunk {
                    paragraph: 1,
                    text: "kenobi".into(),
                    start_ms: 2_000,
                    end_ms: 2_600,
                },
            ],
        )
        .await
        .unwrap();
        let path = sessions::recording_path(dir, sid, r.recording.id);
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, vec![0u8; 96_000]).await.unwrap();
        db::finish_recording(pool, r.recording.id, 3_000)
            .await
            .unwrap();
        sid
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn list_pages_newest_first(pool: PgPool) {
        let f = fixture(pool);
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(seed(&f.pool, f.dir.path(), None).await);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let (status, _, body) = call(&f.app, "GET", "/sessions?limit=2", None).await;
        assert_eq!(status, StatusCode::OK);
        let page = json(&body);
        assert_eq!(page[0]["id"], ids[2].to_string());
        assert_eq!(page[1]["id"], ids[1].to_string());
        assert_eq!(page[0]["preview"], "hello there general kenobi");
        assert_eq!(page[0]["live"], false);
        assert_eq!(page[0]["durationMs"], 3_000);

        let (_, _, body) = call(
            &f.app,
            "GET",
            &format!("/sessions?limit=2&before={}", ids[1]),
            None,
        )
        .await;
        let next = json(&body);
        assert_eq!(next.as_array().unwrap().len(), 1);
        assert_eq!(next[0]["id"], ids[0].to_string());
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn detail_groups_paragraphs(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let (status, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}"), None).await;
        assert_eq!(status, StatusCode::OK);
        let d = json(&body);
        assert_eq!(d["paragraphs"][0]["text"], "hello there general");
        assert_eq!(d["paragraphs"][0]["startMs"], 0);
        assert_eq!(d["paragraphs"][0]["endMs"], 1_500);
        assert_eq!(d["paragraphs"][1]["index"], 1);
        assert!(d["paragraphs"][0].get("chunks").is_none());
        assert_eq!(d["recordings"].as_array().unwrap().len(), 1);
        assert!(d["recordings"][0].get("sessionId").is_none());

        let (status, _, body) = call(
            &f.app,
            "GET",
            &format!("/sessions/{}", Uuid::now_v7()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json(&body)["error"]["code"], "not_found");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn rename_trims_clears_and_validates(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let uri = format!("/sessions/{sid}");

        let (status, _, body) = call(
            &f.app,
            "PATCH",
            &uri,
            Some(serde_json::json!({"title": "  Standup  "})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json(&body)["title"], "Standup");

        let (_, _, body) = call(
            &f.app,
            "PATCH",
            &uri,
            Some(serde_json::json!({"title": "   "})),
        )
        .await;
        assert_eq!(json(&body)["title"], Value::Null);

        let (status, _, body) = call(
            &f.app,
            "PATCH",
            &uri,
            Some(serde_json::json!({"title": "x".repeat(201)})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json(&body)["error"]["code"], "title_too_long");

        let (status, _, _) = call(
            &f.app,
            "PATCH",
            &format!("/sessions/{}", Uuid::now_v7()),
            Some(serde_json::json!({"title": "x"})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn delete_refuses_live_then_removes_rows_and_files(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let uri = format!("/sessions/{sid}");

        let guard = f.live.try_claim(sid).unwrap();
        let (status, _, body) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(json(&body)["error"]["code"], "session_live");
        drop(guard);

        let (status, _, _) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(!sessions::session_dir(f.dir.path(), sid).exists());
        let (status, _, _) = call(&f.app, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _, _) = call(&f.app, "DELETE", &uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn exports_each_format(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), Some("Standup")).await;
        let get = |format: &'static str| {
            let app = f.app.clone();
            async move {
                call(
                    &app,
                    "GET",
                    &format!("/sessions/{sid}/export/{format}"),
                    None,
                )
                .await
            }
        };

        let (status, headers, body) = get("txt").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        assert_eq!(
            headers["content-disposition"],
            r#"attachment; filename="standup.txt""#
        );
        assert_eq!(&body[..], b"hello there general\n\nkenobi\n");

        let (_, headers, body) = get("srt").await;
        assert_eq!(
            headers["content-type"],
            "application/x-subrip; charset=utf-8"
        );
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "1\n00:00:00,000 --> 00:00:01,500\nhello there general\n\n2\n00:00:02,000 --> 00:00:02,600\nkenobi\n\n"
        );

        let (_, headers, body) = get("vtt").await;
        assert_eq!(headers["content-type"], "text/vtt; charset=utf-8");
        assert!(body.starts_with(b"WEBVTT\n\n00:00:00.000 --> 00:00:01.500\n"));

        let (_, headers, body) = get("json").await;
        assert_eq!(headers["content-type"], "application/json");
        let doc = json(&body);
        assert_eq!(doc["session"]["title"], "Standup");
        assert_eq!(doc["paragraphs"][0]["chunks"].as_array().unwrap().len(), 2);
        assert_eq!(doc["recordings"][0]["durationMs"], 3_000);

        let (_, headers, body) = get("wav").await;
        assert_eq!(headers["content-type"], "audio/wav");
        assert_eq!(body.len(), 44 + 96_000);
        assert_eq!(&body[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(body[40..44].try_into().unwrap()), 96_000);

        let (status, _, _) = get("mp3").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn wav_export_repairs_unclosed_recording(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        // A crash during a second recording: 100 ms on disk, never finished.
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        tokio::fs::write(
            sessions::recording_path(f.dir.path(), sid, r.recording.id),
            vec![0u8; 3_200],
        )
        .await
        .unwrap();

        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(body.len(), 44 + 96_000 + 3_200);
        assert_eq!(
            db::get_session(&f.pool, sid)
                .await
                .unwrap()
                .unwrap()
                .duration_ms,
            3_100
        );
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn live_export_does_not_repair_and_drops_half_samples(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        tokio::fs::write(
            sessions::recording_path(f.dir.path(), sid, r.recording.id),
            vec![0u8; 3_201],
        )
        .await
        .unwrap();
        let _guard = f.live.try_claim(sid).unwrap();

        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(body.len(), 44 + 96_000 + 3_200);
        let recs = db::list_recordings(&f.pool, sid).await.unwrap();
        assert_eq!(recs[1].duration_ms, None, "live recording left alone");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn empty_session_wav_is_header_only(pool: PgPool) {
        let f = fixture(pool);
        let sid = Uuid::now_v7();
        db::create_session(&f.pool, sid, "en").await.unwrap();
        let (status, _, body) =
            call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 44);
        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/txt"), None).await;
        assert!(body.is_empty());
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn wav_concatenates_recordings_in_order(pool: PgPool) {
        let f = fixture(pool);
        let sid = Uuid::now_v7();
        db::create_session(&f.pool, sid, "en").await.unwrap();
        for (fill, len) in [(1u8, 640usize), (2u8, 320usize)] {
            let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000)
                .await
                .unwrap();
            let path = sessions::recording_path(f.dir.path(), sid, r.recording.id);
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&path, vec![fill; len]).await.unwrap();
            db::finish_recording(&f.pool, r.recording.id, (len / 32) as i64)
                .await
                .unwrap();
        }
        let (_, _, body) = call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(body.len(), 44 + 960);
        assert!(body[44..44 + 640].iter().all(|&b| b == 1));
        assert!(body[44 + 640..].iter().all(|&b| b == 2));
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn wav_rejects_mixed_sample_rates(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 48_000)
            .await
            .unwrap();
        db::finish_recording(&f.pool, r.recording.id, 0)
            .await
            .unwrap();
        let (status, _, body) =
            call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json(&body)["error"]["code"], "mixed_sample_rates");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn wav_skips_recording_with_missing_file(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let rec = &db::list_recordings(&f.pool, sid).await.unwrap()[0];
        tokio::fs::remove_file(sessions::recording_path(f.dir.path(), sid, rec.id))
            .await
            .unwrap();
        let (status, _, body) =
            call(&f.app, "GET", &format!("/sessions/{sid}/export/wav"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 44);
    }

    /// Runs `uri` while a row lock stalls the handler inside its database
    /// work, and checks the session stays claimed for that whole time.
    async fn assert_claimed_while_blocked(
        f: &Fixture,
        sid: Uuid,
        method: &'static str,
        uri: String,
    ) {
        let mut tx = f.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM sessions WHERE id = $1 FOR UPDATE")
            .bind(sid)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let app = f.app.clone();
        let handle = tokio::spawn(async move { call(&app, method, &uri, None).await.0 });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !handle.is_finished(),
            "handler should be stalled on the row lock"
        );
        assert!(
            f.live.is_live(sid),
            "claim must be held while the handler works"
        );
        tx.commit().await.unwrap();
        assert!(handle.await.unwrap().is_success());
        assert!(!f.live.is_live(sid), "claim released afterwards");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn export_holds_claim_during_repair(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        let r = db::begin_recording(&f.pool, sid, Uuid::now_v7(), 16_000)
            .await
            .unwrap();
        tokio::fs::write(
            sessions::recording_path(f.dir.path(), sid, r.recording.id),
            vec![0u8; 3_200],
        )
        .await
        .unwrap();
        assert_claimed_while_blocked(&f, sid, "GET", format!("/sessions/{sid}/export/wav")).await;
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn delete_holds_claim_while_deleting(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        assert_claimed_while_blocked(&f, sid, "DELETE", format!("/sessions/{sid}")).await;
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn bad_input_gets_json_errors(pool: PgPool) {
        let f = fixture(pool);
        let sid = seed(&f.pool, f.dir.path(), None).await;
        for (method, uri, body) in [
            ("GET", "/sessions/not-a-uuid".to_string(), None),
            ("DELETE", "/sessions/not-a-uuid".to_string(), None),
            ("GET", "/sessions/not-a-uuid/export/txt".to_string(), None),
            ("GET", "/sessions?before=nope".to_string(), None),
            (
                "PATCH",
                format!("/sessions/{sid}"),
                Some(serde_json::json!({"title": 5})),
            ),
        ] {
            let (status, headers, bytes) = call(&f.app, method, &uri, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
            assert_eq!(headers["content-type"], "application/json");
            assert_eq!(json(&bytes)["error"]["code"], "bad_request");
        }
        // Not JSON at all (no content-type, plain text body).
        let req = Request::builder()
            .method("PATCH")
            .uri(format!("/sessions/{sid}"))
            .body(Body::from("hi"))
            .unwrap();
        let res = f.app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(json(&bytes)["error"]["code"], "bad_request");
    }

    #[sqlx::test(migrator = "crate::db::MIGRATOR")]
    async fn unknown_route_is_json_404(pool: PgPool) {
        let f = fixture(pool);
        let (status, _, body) = call(&f.app, "GET", "/nope", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json(&body)["error"]["code"], "not_found");
    }
}
