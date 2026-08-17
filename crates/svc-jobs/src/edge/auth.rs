use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use br_core_auth::{Passport, PassportHeader};
use br_util_graphql::EdgeError;

use super::refusal;

const HEADER: &str = "X-Passport";

pub async fn passport_layer(mut request: Request<Body>, next: Next) -> Response {
    let Some(header) = request
        .headers()
        .get(HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    else {
        return unauthenticated("the trusted passport header is absent");
    };
    let Ok(passport) = Passport::from_header(header) else {
        return unauthenticated("the trusted passport header could not be decoded");
    };
    request.extensions_mut().insert(passport);
    next.run(request).await
}

fn unauthenticated(reason: &str) -> Response {
    tracing::warn!(reason, "X-Passport rejected");
    refusal(StatusCode::UNAUTHORIZED, EdgeError::unauthenticated())
}
