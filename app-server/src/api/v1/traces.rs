use std::sync::Arc;

use actix_web::{HttpRequest, HttpResponse, post, web};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    db::{DB, project_api_keys::ProjectApiKey, spans::Span},
    features::{Feature, is_feature_enabled},
    mq::MessageQueue,
    opentelemetry_json::decode_export_trace_service_request as decode_json_request,
    opentelemetry_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest,
    routes::types::ResponseResult,
    traces::producer::push_spans_to_queue,
    utils::limits::get_workspace_bytes_limit_exceeded,
};
use prost::Message;

#[derive(Serialize, Deserialize, Clone)]
pub struct RabbitMqSpanMessage {
    pub span: Span,
}

// /v1/traces
#[post("")]
pub async fn process_traces(
    req: HttpRequest,
    body: Bytes,
    project_api_key: ProjectApiKey,
    cache: web::Data<crate::cache::Cache>,
    spans_message_queue: web::Data<Arc<MessageQueue>>,
    db: web::Data<DB>,
    clickhouse: web::Data<clickhouse::Client>,
) -> ResponseResult {
    let db = db.into_inner();
    let cache = cache.into_inner();
    let request = decode_export_trace_request(&req, body)?;
    let spans_message_queue = spans_message_queue.as_ref().clone();

    if is_feature_enabled(Feature::UsageLimit) {
        let bytes_limit_exceeded = get_workspace_bytes_limit_exceeded(
            db.clone(),
            clickhouse.into_inner().as_ref().clone(),
            cache.clone(),
            project_api_key.project_id,
        )
        .await
        .map_err(|e| {
            log::error!("Failed to get workspace limits: {:?}", e);
        });

        if bytes_limit_exceeded.is_ok_and(|exceeded| exceeded) {
            return Ok(HttpResponse::Forbidden().json("Workspace data limit exceeded"));
        }
    }

    let response = push_spans_to_queue(
        request,
        project_api_key.project_id,
        spans_message_queue,
        db,
        cache,
    )
    .await?;
    if response.partial_success.is_some() {
        return Err(anyhow::anyhow!("There has been an error during trace processing.").into());
    }

    let keep_alive = req.headers().get("connection").map_or(false, |v| {
        v.to_str().unwrap_or_default().trim().to_lowercase() == "keep-alive"
    });
    if keep_alive {
        Ok(HttpResponse::Ok().keep_alive().finish())
    } else {
        Ok(HttpResponse::Ok().finish())
    }
}

/// Dispatch on `Content-Type`: `application/json` is OTLP/HTTP+JSON, anything else
/// (including missing) falls through to OTLP/HTTP+protobuf — matches what every
/// existing SDK sends today.
fn decode_export_trace_request(
    req: &HttpRequest,
    body: Bytes,
) -> Result<ExportTraceServiceRequest, anyhow::Error> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if content_type.starts_with("application/json") {
        decode_json_request(&body).map_err(|e| {
            anyhow::anyhow!("Failed to decode OTLP/JSON ExportTraceServiceRequest: {e}")
        })
    } else {
        ExportTraceServiceRequest::decode(body).map_err(|e| {
            anyhow::anyhow!("Failed to decode ExportTraceServiceRequest from bytes. {e}")
        })
    }
}
