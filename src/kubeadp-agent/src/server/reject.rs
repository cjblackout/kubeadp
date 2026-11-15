//! JSON-shaped error responses for axum extractor rejections.
//!
//! Handlers use the [`ApiJson`], [`ApiQuery`] and [`ApiPath`] wrappers
//! instead of axum's built-in extractors so that a rejection (invalid JSON,
//! a missing/incorrect content type, an oversized body, a query-string or
//! path deserialization failure) produces the same `{"error","code"}`
//! JSON body the rest of the API uses, keeping the status code axum itself
//! chose (400 invalid body / query, 413 too large, 415 unsupported media
//! type, 422 well-formed JSON with bad fields).

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::de::DeserializeOwned;
use serde_json::json;

/// A request that axum rejected while extracting one of the wrapper
/// extractors. Renders as `{"error","code"}` with the status axum chose.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
    code: &'static str,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>, code: &'static str) -> Self {
        Self {
            status,
            message: message.into(),
            code,
        }
    }

    /// Maps a [`JsonRejection`] onto the standard error shape. Oversized
    /// bodies surface through [`JsonRejection::BytesRejection`] (413).
    fn from_json(rejection: axum::extract::rejection::JsonRejection) -> Self {
        use axum::extract::rejection::JsonRejection;
        match rejection {
            JsonRejection::JsonSyntaxError(err) => Self::new(
                StatusCode::BAD_REQUEST,
                format!("request body is not valid JSON ({})", err.body_text()),
                "invalid_body",
            ),
            JsonRejection::JsonDataError(err) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "request body has missing or invalid fields ({})",
                    err.body_text()
                ),
                "invalid_body",
            ),
            JsonRejection::MissingJsonContentType(_) => Self::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "request body must have content type application/json",
                "invalid_content_type",
            ),
            JsonRejection::BytesRejection(err) => Self::from_bytes(err),
            _ => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid request body",
                "invalid_body",
            ),
        }
    }

    /// Maps a [`BytesRejection`] (an oversized body) to 413.
    fn from_bytes(rejection: axum::extract::rejection::BytesRejection) -> Self {
        Self::new(rejection.status(), "request body is too large", "too_large")
    }

    fn from_query(rejection: QueryRejection) -> Self {
        Self::new(
            rejection.status(),
            format!("invalid query string ({})", rejection.body_text()),
            "invalid_query",
        )
    }

    fn from_path(rejection: PathRejection) -> Self {
        Self::new(
            rejection.status(),
            format!("invalid path parameter ({})", rejection.body_text()),
            "invalid_path",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": self.message, "code": self.code })),
        )
            .into_response()
    }
}

/// A `Json<T>` replacement whose rejection is [`ApiError`] (JSON-shaped).
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
    Json<T>: FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(ApiError::from_json)?;
        Ok(ApiJson(value))
    }
}

/// A `Query<T>` replacement whose rejection is [`ApiError`] (JSON-shaped).
pub struct ApiQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
    Query<T>: FromRequestParts<S, Rejection = QueryRejection>,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(ApiError::from_query)?;
        Ok(ApiQuery(value))
    }
}

/// A `Path<T>` replacement whose rejection is [`ApiError`] (JSON-shaped).
pub struct ApiPath<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
    Path<T>: FromRequestParts<S, Rejection = PathRejection>,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Path(value) = Path::<T>::from_request_parts(parts, state)
            .await
            .map_err(ApiError::from_path)?;
        Ok(ApiPath(value))
    }
}
