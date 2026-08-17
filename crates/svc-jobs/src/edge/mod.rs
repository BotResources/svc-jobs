pub mod auth;
pub mod error;
pub mod log_page;
pub mod mutation;
pub mod project;
pub mod query;
pub mod state;
pub mod subscription;
pub mod tree;
pub mod types;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_graphql::http::GraphiQLSource;
use async_graphql::{Request, Schema, SchemaBuilder};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::{Extension, Json};
use br_core_auth::Passport;
use br_util_graphql::EdgeError;
use futures::StreamExt;
use serde_json::{Value, json};

use mutation::MutationRoot;
use query::QueryRoot;
use state::{EdgeState, is_platform_administrator};
use subscription::SubscriptionRoot;

pub type JobsSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn schema_builder() -> SchemaBuilder<QueryRoot, MutationRoot, SubscriptionRoot> {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
}

pub fn build_schema(state: EdgeState) -> JobsSchema {
    schema_builder().data(state).finish()
}

pub fn sdl() -> String {
    schema_builder().finish().sdl()
}

#[derive(Clone)]
pub struct HttpState {
    pub schema: Arc<OnceLock<JobsSchema>>,
}

pub async fn graphql_route(
    State(state): State<HttpState>,
    passport: Option<Extension<Passport>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(Extension(passport)) = passport else {
        return refusal(StatusCode::UNAUTHORIZED, EdgeError::unauthenticated());
    };
    if !is_platform_administrator(&passport) {
        return refusal(StatusCode::FORBIDDEN, EdgeError::forbidden());
    }
    let Some(schema) = state.schema.get() else {
        return refusal(
            StatusCode::SERVICE_UNAVAILABLE,
            EdgeError::internal("not_ready"),
        );
    };
    let request = match parse(&body) {
        Ok(request) => request.data(passport),
        Err(response) => return *response,
    };
    if wants_event_stream(&headers) {
        let stream = schema.execute_stream(request).map(|response| {
            let rendered = serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_owned());
            Ok::<Event, std::convert::Infallible>(Event::default().event("next").data(rendered))
        });
        return Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response();
    }
    Json(schema.execute(request).await).into_response()
}

pub async fn playground() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

fn parse(body: &[u8]) -> Result<Request, Box<Response>> {
    let parsed: Value = serde_json::from_slice(body).map_err(|_| {
        Box::new(refusal(
            StatusCode::BAD_REQUEST,
            EdgeError::bad_user_input().with_reason("malformed_request"),
        ))
    })?;
    let query = parsed["query"].as_str().ok_or_else(|| {
        Box::new(refusal(
            StatusCode::BAD_REQUEST,
            EdgeError::bad_user_input().with_reason("missing_query"),
        ))
    })?;
    let mut request = Request::new(query);
    if let Some(name) = parsed["operationName"].as_str() {
        request = request.operation_name(name);
    }
    if !parsed["variables"].is_null() {
        request = request.variables(async_graphql::Variables::from_json(
            parsed["variables"].clone(),
        ));
    }
    Ok(request)
}

pub(crate) fn refusal(status: StatusCode, error: EdgeError) -> Response {
    let code = error.code().as_str();
    let reason = error.reason_code().map(str::to_owned);
    let mut extensions = json!({ "code": code });
    if let Some(reason) = reason {
        extensions["reason"] = json!(reason);
    }
    (
        status,
        Json(json!({
            "data": Value::Null,
            "errors": [{ "message": code, "extensions": extensions }],
        })),
    )
        .into_response()
}

fn wants_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"))
}
