//! Thin helpers around Connect unary calls.

use crate::errors::Result;
use crate::transport::Factory;
use buffa::Message;
use connectrpc::ConnectError;
use connectrpc::client::{CallOptions, UnaryResponse};
use std::future::Future;

#[inline]
pub async fn await_public<R>(
    call: impl Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
) -> Result<UnaryResponse<R>> {
    call.await.map_err(Factory::map_error)
}

#[inline]
pub async fn await_auth<M, R, Fut>(
    factory: &Factory,
    procedure: &str,
    request: M,
    call: impl FnOnce(M, CallOptions) -> Fut,
) -> Result<UnaryResponse<R>>
where
    M: Message,
    Fut: Future<Output = std::result::Result<UnaryResponse<R>, ConnectError>>,
{
    let opts = factory.sign_options(procedure, &request)?;
    call(request, opts).await.map_err(Factory::map_error)
}
