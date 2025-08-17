// Copyright 2020 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use crate::{
    server::{Channel, Config},
    Response, ServerError,
};
use futures::{prelude::*, ready, task::*};
use pin_project::pin_project;
use std::{io, pin::Pin};

/// A [`Channel`] that limits the number of concurrent requests by throttling.
///
/// Note that this is a very basic throttling heuristic. It is easy to set a number that is too low
/// for the resources available to the server. For production use cases, a more advanced throttler
/// is likely needed.
#[pin_project]
#[derive(Debug)]
pub struct MaxRequests<C> {
    max_in_flight_requests: usize,
    #[pin]
    inner: C,
}

impl<C> MaxRequests<C> {
    /// Returns the inner channel.
    pub fn get_ref(&self) -> &C {
        &self.inner
    }
}

impl<C> MaxRequests<C>
where
    C: Channel,
{
    /// Returns a new `MaxRequests` that wraps the given channel and limits concurrent requests to
    /// `max_in_flight_requests`.
    pub fn new(inner: C, max_in_flight_requests: usize) -> Self {
        MaxRequests {
            max_in_flight_requests,
            inner,
        }
    }
}

impl<C> Stream for MaxRequests<C>
where
    C: Channel,
{
    type Item = <C as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        while self.as_mut().in_flight_requests() >= *self.as_mut().project().max_in_flight_requests
        {
            ready!(self.as_mut().project().inner.poll_ready(cx)?);

            match ready!(self.as_mut().project().inner.poll_next(cx)?) {
                Some(r) => {
                    let _entered = r.span.enter();
                    tracing::info!(
                        in_flight_requests = self.as_mut().in_flight_requests(),
                        "ThrottleRequest",
                    );

                    self.as_mut().start_send(Response {
                        request_id: r.request.id,
                        message: Err(ServerError {
                            kind: io::ErrorKind::WouldBlock,
                            detail: "server throttled the request.".into(),
                        }),
                    })?;
                }
                None => return Poll::Ready(None),
            }
        }
        self.project().inner.poll_next(cx)
    }
}

impl<C> Sink<Response<<C as Channel>::Resp>> for MaxRequests<C>
where
    C: Channel,
{
    type Error = C::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Response<<C as Channel>::Resp>,
    ) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

impl<C> AsRef<C> for MaxRequests<C> {
    fn as_ref(&self) -> &C {
        &self.inner
    }
}

impl<C> Channel for MaxRequests<C>
where
    C: Channel,
{
    type Req = <C as Channel>::Req;
    type Resp = <C as Channel>::Resp;
    type Transport = <C as Channel>::Transport;

    fn in_flight_requests(&self) -> usize {
        self.inner.in_flight_requests()
    }

    fn config(&self) -> &Config {
        self.inner.config()
    }

    fn transport(&self) -> &Self::Transport {
        self.inner.transport()
    }
}

/// An [`Incoming`](crate::server::incoming::Incoming) stream of channels that enforce limits on
/// the number of in-flight requests.
#[pin_project]
#[derive(Debug)]
pub struct MaxRequestsPerChannel<S> {
    #[pin]
    inner: S,
    max_in_flight_requests: usize,
}

impl<S> MaxRequestsPerChannel<S>
where
    S: Stream,
    <S as Stream>::Item: Channel,
{
    pub(crate) fn new(inner: S, max_in_flight_requests: usize) -> Self {
        Self {
            inner,
            max_in_flight_requests,
        }
    }
}

impl<S> Stream for MaxRequestsPerChannel<S>
where
    S: Stream,
    <S as Stream>::Item: Channel,
{
    type Item = MaxRequests<<S as Stream>::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match ready!(self.as_mut().project().inner.poll_next(cx)) {
            Some(channel) => Poll::Ready(Some(MaxRequests::new(
                channel,
                *self.project().max_in_flight_requests,
            ))),
            None => Poll::Ready(None),
        }
    }
}
