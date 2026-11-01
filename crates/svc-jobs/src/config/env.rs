use std::str::FromStr;

use crate::error::ServiceError;

pub(super) fn declared(key: &'static str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

pub(super) fn parse<T: FromStr>(
    key: &'static str,
    raw: Option<&str>,
    fallback: T,
) -> Result<T, ServiceError> {
    match raw {
        None => Ok(fallback),
        Some(raw) => raw.parse().map_err(|_| ServiceError::Configuration {
            key,
            value: raw.to_owned(),
        }),
    }
}

pub(super) fn read<T: FromStr>(key: &'static str, fallback: T) -> Result<T, ServiceError> {
    parse(key, std::env::var(key).ok().as_deref(), fallback)
}

pub(super) fn required(key: &'static str) -> Result<String, ServiceError> {
    std::env::var(key).map_err(|_| ServiceError::MissingConfiguration { key })
}
