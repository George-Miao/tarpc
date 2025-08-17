// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Provides a server that concurrently handles many connections sending multiplexed requests.

use crate::{
    cancellations::{cancellations, CanceledRequests, RequestCancellation},
    context::{self, SpanExt},
    trace,
    util::TimeUntil,
    ChannelError, ClientMessage, Request, RequestName, Response, ServerError, Transport,
};
use futures::channel::mpsc;
use futures::{
    future::{AbortRegistration, Abortable},
    prelude::*,
    ready,
    stream::Fuse,
    task::*,
};
use in_flight_requests::{AlreadyExistsError, InFlightRequests};
use pin_project::pin_project;
use std::{
    convert::TryFrom, error::Error, fmt, marker::PhantomData, pin::Pin, sync::Arc, time::SystemTime,
};
use tracing::{info_span, instrument::Instrument, Span};

mod in_flight_requests;
pub mod request_hook;
#[cfg(test)]
mod testing;

/// Provides functionality to apply server limits.
pub mod limits;

/// Provides helper methods for streams of Channels.
pub mod incoming;

/// Settings that control the behavior of [channels](Channel).
#[derive(Clone, Debug)]
pub struct Config {
    /// Controls the buffer size of the in-process channel over which a server's handlers send
    /// responses to the [`Channel`]. In other words, this is the number of responses that can sit
    /// in the outbound queue before request handlers begin blocking.
    pub pending_response_buffer: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            pending_response_buffer: 100,
        }
    }
}

impl Config {
    /// Returns a channel backed by `transport` and configured with `self`.
    pub fn channel<Req, Resp, T>(self, transport: T) -> BaseChannel<Req, Resp, T>
    where
        T: Transport<Response<Resp>, ClientMessage<Req>>,
    {
        BaseChannel::new(self, transport)
    }
}

/// Equivalent to a `FnOnce(Req) -> impl Future<Output = Resp>`.
#[allow(async_fn_in_trait)]
pub trait Serve {
    /// Type of request.
    type Req: RequestName;

    /// Type of response.
    type Resp;

    /// Responds to a single request.
    async fn serve(self, ctx: context::Context, req: Self::Req) -> Result<Self::Resp, ServerError>;
}

/// A Serve wrapper around a Fn.
#[derive(Debug)]
pub struct ServeFn<Req, Resp, F> {
    f: F,
    data: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp, F> Clone for ServeFn<Req, Resp, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            data: PhantomData,
        }
    }
}

impl<Req, Resp, F> Copy for ServeFn<Req, Resp, F> where F: Copy {}

/// Creates a [`Serve`] wrapper around a `FnOnce(context::Context, Req) -> impl Future<Output =
/// Result<Resp, ServerError>>`.
pub fn serve<Req, Resp, Fut, F>(f: F) -> ServeFn<Req, Resp, F>
where
    F: FnOnce(context::Context, Req) -> Fut,
    Fut: Future<Output = Result<Resp, ServerError>>,
{
    ServeFn {
        f,
        data: PhantomData,
    }
}

impl<Req, Resp, Fut, F> Serve for ServeFn<Req, Resp, F>
where
    Req: RequestName,
    F: FnOnce(context::Context, Req) -> Fut,
    Fut: Future<Output = Result<Resp, ServerError>>,
{
    type Req = Req;
    type Resp = Resp;

    async fn serve(self, ctx: context::Context, req: Req) -> Result<Resp, ServerError> {
        (self.f)(ctx, req).await
    }
}

/// BaseChannel is the standard implementation of a [`Channel`].
///
/// BaseChannel manages a [`Transport`](Transport) of client [`messages`](ClientMessage) and
/// implements a [`Stream`] of [requests](TrackedRequest). See the [`Channel`] documentation for
/// how to use channels.
///
/// Besides requests, the other type of client message handled by `BaseChannel` is [cancellation
/// messages](ClientMessage::Cancel). `BaseChannel` does not allow direct access to cancellation
/// messages. Instead, it internally handles them by cancelling corresponding requests (removing
/// the corresponding in-flight requests and aborting their handlers).
#[pin_project]
pub struct BaseChannel<Req, Resp, T> {
    config: Config,
    /// Writes responses to the wire and reads requests off the wire.
    #[pin]
    transport: Fuse<T>,
    /// In-flight requests that were dropped by the server before completion.
    #[pin]
    canceled_requests: CanceledRequests,
    /// Notifies `canceled_requests` when a request is canceled.
    request_cancellation: RequestCancellation,
    /// Holds data necessary to clean up in-flight requests.
    in_flight_requests: InFlightRequests,
    /// Types the request and response.
    ghost: PhantomData<(fn() -> Req, fn(Resp))>,
}

impl<Req, Resp, T> BaseChannel<Req, Resp, T>
where
    T: Transport<Response<Resp>, ClientMessage<Req>>,
{
    /// Creates a new channel backed by `transport` and configured with `config`.
    pub fn new(config: Config, transport: T) -> Self {
        let (request_cancellation, canceled_requests) = cancellations();
        BaseChannel {
            config,
            transport: transport.fuse(),
            canceled_requests,
            request_cancellation,
            in_flight_requests: InFlightRequests::default(),
            ghost: PhantomData,
        }
    }

    /// Creates a new channel backed by `transport` and configured with the defaults.
    pub fn with_defaults(transport: T) -> Self {
        Self::new(Config::default(), transport)
    }

    /// Returns the inner transport over which messages are sent and received.
    pub fn get_ref(&self) -> &T {
        self.transport.get_ref()
    }

    /// Returns the inner transport over which messages are sent and received.
    pub fn get_pin_ref(self: Pin<&mut Self>) -> Pin<&mut T> {
        self.project().transport.get_pin_mut()
    }

    fn in_flight_requests_mut<'a>(self: &'a mut Pin<&mut Self>) -> &'a mut InFlightRequests {
        self.as_mut().project().in_flight_requests
    }

    fn canceled_requests_pin_mut<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> Pin<&'a mut CanceledRequests> {
        self.as_mut().project().canceled_requests
    }

    fn transport_pin_mut<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut Fuse<T>> {
        self.as_mut().project().transport
    }

    fn start_request(
        mut self: Pin<&mut Self>,
        mut request: Request<Req>,
    ) -> Result<TrackedRequest<Req>, AlreadyExistsError> {
        let span = info_span!(
            "RPC",
            rpc.trace_id = %request.context.trace_id(),
            rpc.deadline = %humantime::format_rfc3339(SystemTime::now() + request.context.deadline.time_until()),
            otel.kind = "server",
            otel.name = tracing::field::Empty,
        );
        span.set_context(&request.context);
        request.context.trace_context = trace::Context::try_from(&span).unwrap_or_else(|_| {
            tracing::trace!(
                "OpenTelemetry subscriber not installed; making unsampled \
                            child context."
            );
            request.context.trace_context.new_child()
        });
        let entered = span.enter();
        tracing::info!("ReceiveRequest");
        let start = self.in_flight_requests_mut().start_request(
            request.id,
            request.context.deadline,
            span.clone(),
        );
        match start {
            Ok(abort_registration) => {
                drop(entered);
                Ok(TrackedRequest {
                    abort_registration,
                    span,
                    response_guard: ResponseGuard {
                        request_id: request.id,
                        request_cancellation: self.request_cancellation.clone(),
                        cancel: false,
                    },
                    request,
                })
            }
            Err(AlreadyExistsError) => {
                tracing::trace!("DuplicateRequest");
                Err(AlreadyExistsError)
            }
        }
    }
}

impl<Req, Resp, T> fmt::Debug for BaseChannel<Req, Resp, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BaseChannel")
    }
}

/// A request tracked by a [`Channel`].
#[derive(Debug)]
pub struct TrackedRequest<Req> {
    /// The request sent by the client.
    pub request: Request<Req>,
    /// A registration to abort a future when the [`Channel`] that produced this request stops
    /// tracking it.
    pub abort_registration: AbortRegistration,
    /// A span representing the server processing of this request.
    pub span: Span,
    /// An inert response guard. Becomes active in an InFlightRequest.
    pub response_guard: ResponseGuard,
}

pub trait Channel
where
    Self: Transport<Response<<Self as Channel>::Resp>, TrackedRequest<<Self as Channel>::Req>>,
{
    /// Type of request item.
    type Req;

    /// Type of response sink item.
    type Resp;

    /// The wrapped transport.
    type Transport;

    /// Configuration of the channel.
    fn config(&self) -> &Config;

    /// Returns the number of in-flight requests over this channel.
    fn in_flight_requests(&self) -> usize;

    /// Returns the transport underlying the channel.
    fn transport(&self) -> &Self::Transport;

    /// Caps the number of concurrent requests to `limit`. An error will be returned for requests
    /// over the concurrency limit.
    ///
    /// Note that this is a very
    /// simplistic throttling heuristic. It is easy to set a number that is too low for the
    /// resources available to the server. For production use cases, a more advanced throttler is
    /// likely needed.
    fn max_concurrent_requests(
        self,
        limit: usize,
    ) -> limits::requests_per_channel::MaxRequests<Self>
    where
        Self: Sized,
    {
        limits::requests_per_channel::MaxRequests::new(self, limit)
    }

    fn requests(self) -> Requests<Self>
    where
        Self: Sized,
    {
        let (responses_tx, responses) = mpsc::channel(self.config().pending_response_buffer);

        Requests {
            channel: self,
            pending_responses: responses,
            responses_tx,
        }
    }

    fn execute<S>(self, serve: S) -> impl Stream<Item = impl Future<Output = ()>>
    where
        Self: Sized,
        Self::Req: RequestName,
        S: Serve<Req = Self::Req, Resp = Self::Resp> + Clone,
    {
        self.requests().execute(serve)
    }
}

impl<Req, Resp, T> Stream for BaseChannel<Req, Resp, T>
where
    T: Transport<Response<Resp>, ClientMessage<Req>>,
{
    type Item = Result<TrackedRequest<Req>, ChannelError<T::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        #[derive(Clone, Copy, Debug)]
        enum ReceiverStatus {
            Ready,
            Pending,
            Closed,
        }

        impl ReceiverStatus {
            fn combine(self, other: Self) -> Self {
                use ReceiverStatus::*;
                match (self, other) {
                    (Ready, _) | (_, Ready) => Ready,
                    (Closed, Closed) => Closed,
                    (Pending, Closed) | (Closed, Pending) | (Pending, Pending) => Pending,
                }
            }
        }

        use ReceiverStatus::*;

        loop {
            let cancellation_status = match self.canceled_requests_pin_mut().poll_recv(cx) {
                Poll::Ready(Some(request_id)) => {
                    if let Some(span) = self.in_flight_requests_mut().remove_request(request_id) {
                        let _entered = span.enter();
                        tracing::info!("ResponseCancelled");
                    }
                    Ready
                }
                // Pending cancellations don't block Channel closure, because all they do is ensure
                // the Channel's internal state is cleaned up. But Channel closure also cleans up
                // the Channel state, so there's no reason to wait on a cancellation before
                // closing.
                //
                // Ready(None) can't happen, since `self` holds a Cancellation.
                Poll::Pending | Poll::Ready(None) => Closed,
            };

            let expiration_status = match self.in_flight_requests_mut().poll_expired(cx) {
                // No need to send a response, since the client wouldn't be waiting for one
                // anymore.
                Poll::Ready(Some(_)) => Ready,
                Poll::Ready(None) => Closed,
                Poll::Pending => Pending,
            };

            let request_status = match self
                .transport_pin_mut()
                .poll_next(cx)
                .map_err(|e| ChannelError::Read(Arc::new(e)))?
            {
                Poll::Ready(Some(message)) => match message {
                    ClientMessage::Request(request) => {
                        match self.as_mut().start_request(request) {
                            Ok(request) => return Poll::Ready(Some(Ok(request))),
                            Err(AlreadyExistsError) => {
                                // Instead of closing the channel if a duplicate request is sent,
                                // just ignore it, since it's already being processed. Note that we
                                // cannot return Poll::Pending here, since nothing has scheduled a
                                // wakeup yet.
                                continue;
                            }
                        }
                    }
                    ClientMessage::Cancel {
                        trace_context,
                        request_id,
                    } => {
                        if !self.in_flight_requests_mut().cancel_request(request_id) {
                            tracing::trace!(
                                rpc.trace_id = %trace_context.trace_id,
                                "Received cancellation, but response handler is already complete.",
                            );
                        }
                        Ready
                    }
                },
                Poll::Ready(None) => Closed,
                Poll::Pending => Pending,
            };

            let status = cancellation_status
                .combine(expiration_status)
                .combine(request_status);

            tracing::trace!(
                "Cancellations: {cancellation_status:?}, \
                Expired requests: {expiration_status:?}, \
                Inbound: {request_status:?}, \
                Overall: {status:?}",
            );
            match status {
                Ready => continue,
                Closed => return Poll::Ready(None),
                Pending => return Poll::Pending,
            }
        }
    }
}

impl<Req, Resp, T> Sink<Response<Resp>> for BaseChannel<Req, Resp, T>
where
    T: Transport<Response<Resp>, ClientMessage<Req>>,
    T::Error: Error,
{
    type Error = ChannelError<T::Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project()
            .transport
            .poll_ready(cx)
            .map_err(|e| ChannelError::Ready(Arc::new(e)))
    }

    fn start_send(mut self: Pin<&mut Self>, response: Response<Resp>) -> Result<(), Self::Error> {
        if let Some(span) = self
            .in_flight_requests_mut()
            .remove_request(response.request_id)
        {
            let _entered = span.enter();
            tracing::info!("SendResponse");
            self.project()
                .transport
                .start_send(response)
                .map_err(|e| ChannelError::Write(Arc::new(e)))
        } else {
            // If the request isn't tracked anymore, there's no need to send the response.
            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        tracing::trace!("poll_flush");
        self.project()
            .transport
            .poll_flush(cx)
            .map_err(|e| ChannelError::Flush(Arc::new(e)))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project()
            .transport
            .poll_close(cx)
            .map_err(|e| ChannelError::Close(Arc::new(e)))
    }
}

impl<Req, Resp, T> AsRef<T> for BaseChannel<Req, Resp, T> {
    fn as_ref(&self) -> &T {
        self.transport.get_ref()
    }
}

impl<Req, Resp, T> Channel for BaseChannel<Req, Resp, T>
where
    T: Transport<Response<Resp>, ClientMessage<Req>>,
{
    type Req = Req;
    type Resp = Resp;
    type Transport = T;

    fn config(&self) -> &Config {
        &self.config
    }

    fn in_flight_requests(&self) -> usize {
        self.in_flight_requests.len()
    }

    fn transport(&self) -> &Self::Transport {
        self.get_ref()
    }
}

/// A stream of requests coming over a channel. `Requests` also drives the sending of responses, so
/// it must be continually polled to ensure progress.
#[pin_project]
pub struct Requests<C>
where
    C: Channel,
{
    #[pin]
    channel: C,
    /// Responses waiting to be written to the wire.
    pending_responses: mpsc::Receiver<Response<C::Resp>>,
    /// Handed out to request handlers to fan in responses.
    responses_tx: mpsc::Sender<Response<C::Resp>>,
}

impl<C> Requests<C>
where
    C: Channel,
{
    /// Returns a reference to the inner channel over which messages are sent and received.
    pub fn channel(&self) -> &C {
        &self.channel
    }

    /// Returns the inner channel over which messages are sent and received.
    pub fn channel_pin_mut<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut C> {
        self.as_mut().project().channel
    }

    /// Returns the inner channel over which messages are sent and received.
    pub fn pending_responses_mut<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut mpsc::Receiver<Response<C::Resp>> {
        self.as_mut().project().pending_responses
    }

    fn pump_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<InFlightRequest<C::Req, C::Resp>, C::Error>>> {
        self.channel_pin_mut().poll_next(cx).map_ok(
            |TrackedRequest {
                 request,
                 abort_registration,
                 span,
                 mut response_guard,
             }| {
                // The response guard becomes active once in an InFlightRequest.
                response_guard.cancel = true;
                {
                    let _entered = span.enter();
                    tracing::info!("BeginRequest");
                }
                InFlightRequest {
                    request,
                    abort_registration,
                    span,
                    response_guard,
                    response_tx: self.responses_tx.clone(),
                }
            },
        )
    }

    fn pump_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        read_half_closed: bool,
    ) -> Poll<Option<Result<(), C::Error>>> {
        match self.as_mut().poll_next_response(cx)? {
            Poll::Ready(Some(response)) => {
                // A Ready result from poll_next_response means the Channel is ready to be written
                // to. Therefore, we can call start_send without worry of a full buffer.
                self.channel_pin_mut().start_send(response)?;
                Poll::Ready(Some(Ok(())))
            }
            Poll::Ready(None) => {
                // Shutdown can't be done before we finish pumping out remaining responses.
                ready!(self.channel_pin_mut().poll_flush(cx)?);
                Poll::Ready(None)
            }
            Poll::Pending => {
                // No more requests to process, so flush any requests buffered in the transport.
                ready!(self.channel_pin_mut().poll_flush(cx)?);

                // Being here means there are no staged requests and all written responses are
                // fully flushed. So, if the read half is closed and there are no in-flight
                // requests, then we can close the write half.
                if read_half_closed && self.channel.in_flight_requests() == 0 {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    /// Yields a response ready to be written to the Channel sink.
    ///
    /// Note that a response will only be yielded if the Channel is *ready* to be written to (i.e.
    /// start_send would succeed).
    fn poll_next_response(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Response<C::Resp>, C::Error>>> {
        ready!(self.ensure_writeable(cx)?);

        match ready!(self.pending_responses_mut().poll_next_unpin(cx)) {
            Some(response) => Poll::Ready(Some(Ok(response))),
            None => {
                // This branch likely won't happen, since the Requests stream is holding a Sender.
                Poll::Ready(None)
            }
        }
    }

    /// Returns Ready if writing a message to the Channel would not fail due to a full buffer. If
    /// the Channel is not ready to be written to, flushes it until it is ready.
    fn ensure_writeable<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), C::Error>>> {
        while self.channel_pin_mut().poll_ready(cx)?.is_pending() {
            ready!(self.channel_pin_mut().poll_flush(cx)?);
        }
        Poll::Ready(Some(Ok(())))
    }

    pub fn execute<S>(self, serve: S) -> impl Stream<Item = impl Future<Output = ()>>
    where
        C::Req: RequestName,
        S: Serve<Req = C::Req, Resp = C::Resp> + Clone,
    {
        self.take_while(|result| {
            if let Err(e) = result {
                tracing::warn!("Requests stream errored out: {}", e);
            }
            futures::future::ready(result.is_ok())
        })
        .filter_map(|result| async move { result.ok() })
        .map(move |request| {
            let serve = serve.clone();
            request.execute(serve)
        })
    }
}

impl<C> fmt::Debug for Requests<C>
where
    C: Channel,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Requests")
    }
}

/// A fail-safe to ensure requests are properly canceled if request processing is aborted before
/// completing.
#[derive(Debug)]
pub struct ResponseGuard {
    request_cancellation: RequestCancellation,
    request_id: u64,
    cancel: bool,
}

impl Drop for ResponseGuard {
    fn drop(&mut self) {
        if self.cancel {
            self.request_cancellation.cancel(self.request_id);
        }
    }
}

/// A request produced by [Channel::requests].
///
/// If dropped without calling [`execute`](InFlightRequest::execute), a cancellation message will
/// be sent to the Channel to clean up associated request state.
#[derive(Debug)]
pub struct InFlightRequest<Req, Res> {
    request: Request<Req>,
    abort_registration: AbortRegistration,
    response_guard: ResponseGuard,
    span: Span,
    response_tx: mpsc::Sender<Response<Res>>,
}

impl<Req, Res> InFlightRequest<Req, Res> {
    /// Returns a reference to the request.
    pub fn get(&self) -> &Request<Req> {
        &self.request
    }

    pub async fn execute<S>(self, serve: S)
    where
        Req: RequestName,
        S: Serve<Req = Req, Resp = Res>,
    {
        let Self {
            mut response_tx,
            mut response_guard,
            abort_registration,
            span,
            request:
                Request {
                    context,
                    message,
                    id: request_id,
                },
        } = self;
        span.record("otel.name", message.name());
        let _ = Abortable::new(
            async move {
                let message = serve.serve(context, message).await;
                tracing::info!("CompleteRequest");
                let response = Response {
                    request_id,
                    message,
                };
                let _ = response_tx.send(response).await;
                tracing::info!("BufferResponse");
            },
            abort_registration,
        )
        .instrument(span)
        .await;
        // Request processing has completed, meaning either the channel canceled the request or
        // a request was sent back to the channel. Either way, the channel will clean up the
        // request data, so the request does not need to be canceled.
        response_guard.cancel = false;
    }
}

fn print_err(e: &(dyn Error + 'static)) -> String {
    anyhow::Chain::new(e)
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

impl<C> Stream for Requests<C>
where
    C: Channel,
{
    type Item = Result<InFlightRequest<C::Req, C::Resp>, C::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let read = self.as_mut().pump_read(cx).map_err(|e| {
                tracing::trace!("read: {}", print_err(&e));
                e
            })?;
            let read_closed = matches!(read, Poll::Ready(None));
            let write = self.as_mut().pump_write(cx, read_closed).map_err(|e| {
                tracing::trace!("write: {}", print_err(&e));
                e
            })?;
            match (read, write) {
                (Poll::Ready(None), Poll::Ready(None)) => {
                    tracing::trace!("read: Poll::Ready(None), write: Poll::Ready(None)");
                    return Poll::Ready(None);
                }
                (Poll::Ready(Some(request_handler)), _) => {
                    tracing::trace!("read: Poll::Ready(Some), write: _");
                    return Poll::Ready(Some(Ok(request_handler)));
                }
                (_, Poll::Ready(Some(()))) => {
                    tracing::trace!("read: _, write: Poll::Ready(Some)");
                }
                (read @ Poll::Pending, write) | (read, write @ Poll::Pending) => {
                    tracing::trace!(
                        "read pending: {}, write pending: {}",
                        read.is_pending(),
                        write.is_pending()
                    );
                    return Poll::Pending;
                }
            }
        }
    }
}
