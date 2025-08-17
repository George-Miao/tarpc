// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Provides a client that connects to a server and sends multiplexed requests.

mod in_flight_requests;
pub mod stub;

use crate::{
    cancellations::{cancellations, CanceledRequests, RequestCancellation},
    context, trace,
    util::TimeUntil,
    ChannelError, ClientMessage, Request, RequestName, Response, ServerError, Transport,
};
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::{prelude::*, ready, stream::Fuse, task::*};
use in_flight_requests::InFlightRequests;
use pin_project::pin_project;
use std::{
    any::Any,
    convert::TryFrom,
    fmt,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tracing::Span;

/// Settings that control the behavior of the client.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Config {
    /// The number of requests that can be in flight at once.
    /// `max_in_flight_requests` controls the size of the map used by the client
    /// for storing pending requests.
    pub max_in_flight_requests: usize,
    /// The number of requests that can be buffered client-side before being sent.
    /// `pending_requests_buffer` controls the size of the channel clients use
    /// to communicate with the request dispatch task.
    pub pending_request_buffer: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_in_flight_requests: 1_000,
            pending_request_buffer: 100,
        }
    }
}

/// A channel and dispatch pair. The dispatch drives the sending and receiving of requests
/// and must be polled continuously or spawned.
pub struct NewClient<C, D> {
    /// The new client.
    pub client: C,
    /// The client's dispatch.
    pub dispatch: D,
}

impl<C, D, E> NewClient<C, D>
where
    D: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn spawn(self) -> C {
        let dispatch = self.dispatch.unwrap_or_else(move |e| {
            let e = anyhow::Error::new(e);
            tracing::warn!("Connection broken: {:?}", e);
        });
        n0_future::task::spawn(dispatch);
        self.client
    }
}

impl<C, D> fmt::Debug for NewClient<C, D> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "NewClient")
    }
}

const _CHECK_USIZE: () = assert!(
    std::mem::size_of::<usize>() <= std::mem::size_of::<u64>(),
    "usize is too big to fit in u64"
);

/// Handles communication from the client to request dispatch.
#[derive(Debug)]
pub struct Channel<Req, Resp> {
    to_dispatch: mpsc::Sender<DispatchRequest<Req, Resp>>,
    /// Channel to send a cancel message to the dispatcher.
    cancellation: RequestCancellation,
    /// The ID to use for the next request to stage.
    next_request_id: Arc<AtomicUsize>,
}

impl<Req, Resp> Clone for Channel<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            to_dispatch: self.to_dispatch.clone(),
            cancellation: self.cancellation.clone(),
            next_request_id: self.next_request_id.clone(),
        }
    }
}

impl<Req, Resp> Channel<Req, Resp>
where
    Req: RequestName,
{
    /// Sends a request to the dispatch task to forward to the server, returning a [`Future`] that
    /// resolves to the response.
    #[tracing::instrument(
        name = "RPC",
        skip(self, ctx, request),
        fields(
            rpc.trace_id = tracing::field::Empty,
            rpc.deadline = %humantime::format_rfc3339(SystemTime::now() + ctx.deadline.time_until()),
            otel.kind = "client",
            otel.name = %request.name())
        )]
    pub async fn call(&self, mut ctx: context::Context, request: Req) -> Result<Resp, RpcError> {
        let span = Span::current();
        ctx.trace_context = trace::Context::try_from(&span).unwrap_or_else(|_| {
            tracing::trace!(
                "OpenTelemetry subscriber not installed; making unsampled child context."
            );
            ctx.trace_context.new_child()
        });
        span.record("rpc.trace_id", tracing::field::display(ctx.trace_id()));
        let (response_completion, mut response) = oneshot::channel();
        let request_id =
            u64::try_from(self.next_request_id.fetch_add(1, Ordering::Relaxed)).unwrap();

        // ResponseGuard impls Drop to cancel in-flight requests. It should be created before
        // sending out the request; otherwise, the response future could be dropped after the
        // request is sent out but before ResponseGuard is created, rendering the cancellation
        // logic inactive.
        let response_guard = ResponseGuard {
            response: &mut response,
            request_id,
            cancellation: &self.cancellation,
            cancel: true,
        };
        self.to_dispatch
            .clone()
            .send(DispatchRequest {
                ctx,
                span,
                request_id,
                request,
                response_completion,
            })
            .await
            .map_err(|_| RpcError::Shutdown)?;
        response_guard.response().await
    }
}

/// A server response that is completed by request dispatch when the corresponding response
/// arrives off the wire.
struct ResponseGuard<'a, Resp> {
    response: &'a mut oneshot::Receiver<Result<Resp, RpcError>>,
    cancellation: &'a RequestCancellation,
    request_id: u64,
    cancel: bool,
}

/// An error that can occur in the processing of an RPC. This is not request-specific errors but
/// rather cross-cutting errors that can always occur.
#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    /// The client disconnected from the server.
    #[error("the connection to the server was already shutdown")]
    Shutdown,
    /// The client failed to buffer the request in the underlying transport.
    #[error("the client failed to buffer the request in the underlying transport")]
    Send(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// The channel was disconnected due to a critical error.
    #[error("the channel was disconnected due to a critical error")]
    Channel(#[source] ChannelError<dyn std::error::Error + Send + Sync + 'static>),
    /// The request exceeded its deadline.
    #[error("the request exceeded its deadline")]
    DeadlineExceeded,
    /// The server aborted request processing.
    #[error("the server aborted request processing")]
    Server(#[from] ServerError),
}

impl<Resp> ResponseGuard<'_, Resp> {
    async fn response(mut self) -> Result<Resp, RpcError> {
        let response = (&mut self.response).await;
        // Cancel drop logic once a response has been received.
        self.cancel = false;
        match response {
            Ok(response) => response,
            Err(_) => {
                // The oneshot is Canceled when the dispatch task ends. In that case,
                // there's nothing listening on the other side, so there's no point in
                // propagating cancellation.
                Err(RpcError::Shutdown)
            }
        }
    }
}

// Cancels the request when dropped, if not already complete.
impl<Resp> Drop for ResponseGuard<'_, Resp> {
    fn drop(&mut self) {
        // The receiver needs to be closed to handle the edge case that the request has not
        // yet been received by the dispatch task. It is possible for the cancel message to
        // arrive before the request itself, in which case the request could get stuck in the
        // dispatch map forever if the server never responds (e.g. if the server dies while
        // responding). Even if the server does respond, it will have unnecessarily done work
        // for a client no longer waiting for a response. To avoid this, the dispatch task
        // checks if the receiver is closed before inserting the request in the map. By
        // closing the receiver before sending the cancel message, it is guaranteed that if the
        // dispatch task misses an early-arriving cancellation message, then it will see the
        // receiver as closed.
        self.response.close();
        if self.cancel {
            self.cancellation.cancel(self.request_id);
        }
    }
}

/// Returns a channel and dispatcher that manages the lifecycle of requests initiated by the
/// channel.
pub fn new<Req, Resp, C>(
    config: Config,
    transport: C,
) -> NewClient<Channel<Req, Resp>, RequestDispatch<Req, Resp, C>>
where
    C: Transport<ClientMessage<Req>, Response<Resp>>,
{
    let (to_dispatch, pending_requests) = mpsc::channel(config.pending_request_buffer);
    let (cancellation, canceled_requests) = cancellations();

    NewClient {
        client: Channel {
            to_dispatch,
            cancellation,
            next_request_id: Arc::new(AtomicUsize::new(0)),
        },
        dispatch: RequestDispatch {
            config,
            canceled_requests,
            transport: transport.fuse(),
            in_flight_requests: InFlightRequests::default(),
            pending_requests,
            terminal_error: None,
        },
    }
}

/// Handles the lifecycle of requests, writing requests to the wire, managing cancellations,
/// and dispatching responses to the appropriate channel.
#[must_use]
#[pin_project()]
#[derive(Debug)]
pub struct RequestDispatch<Req, Resp, C> {
    /// Writes requests to the wire and reads responses off the wire.
    #[pin]
    transport: Fuse<C>,
    /// Requests waiting to be written to the wire.
    pending_requests: mpsc::Receiver<DispatchRequest<Req, Resp>>,
    /// Requests that were dropped.
    canceled_requests: CanceledRequests,
    /// Requests already written to the wire that haven't yet received responses.
    in_flight_requests: InFlightRequests<Result<Resp, RpcError>>,
    /// Configures limits to prevent unlimited resource usage.
    config: Config,
    /// Produces errors that can be sent in response to any unprocessed requests at the time
    /// RequestDispatch is dropped. Correctness note: this field should only be populated by
    /// RequestDispatch::poll, which relies on downcasting the Any to a concrete error type
    /// determined within the poll function.
    terminal_error: Option<ChannelError<dyn Any + Send + Sync + 'static>>,
}

impl<Req, Resp, C> RequestDispatch<Req, Resp, C>
where
    C: Transport<ClientMessage<Req>, Response<Resp>>,
{
    fn in_flight_requests<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut InFlightRequests<Result<Resp, RpcError>> {
        self.as_mut().project().in_flight_requests
    }

    fn transport_pin_mut<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut Fuse<C>> {
        self.as_mut().project().transport
    }

    fn poll_ready<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ChannelError<C::Error>>> {
        self.transport_pin_mut()
            .poll_ready(cx)
            .map_err(|e| ChannelError::Ready(Arc::new(e)))
    }

    fn start_send(self: &mut Pin<&mut Self>, message: ClientMessage<Req>) -> Result<(), C::Error> {
        self.transport_pin_mut().start_send(message)
    }

    fn poll_flush<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ChannelError<C::Error>>> {
        self.transport_pin_mut()
            .poll_flush(cx)
            .map_err(|e| ChannelError::Flush(Arc::new(e)))
    }

    fn poll_close<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ChannelError<C::Error>>> {
        self.transport_pin_mut()
            .poll_close(cx)
            .map_err(|e| ChannelError::Close(Arc::new(e)))
    }

    fn canceled_requests_mut<'a>(self: &'a mut Pin<&mut Self>) -> &'a mut CanceledRequests {
        self.as_mut().project().canceled_requests
    }

    fn pending_requests_mut<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut mpsc::Receiver<DispatchRequest<Req, Resp>> {
        self.as_mut().project().pending_requests
    }

    fn terminal_error_mut<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut Option<ChannelError<dyn Any + Send + Sync + 'static>> {
        self.as_mut().project().terminal_error
    }

    fn pump_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), ChannelError<C::Error>>>> {
        self.transport_pin_mut()
            .poll_next(cx)
            .map_err(|e| ChannelError::Read(Arc::new(e)))
            .map_ok(|response| {
                self.complete(response);
            })
    }

    fn pump_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), ChannelError<C::Error>>>> {
        enum ReceiverStatus {
            Pending,
            Closed,
        }

        let pending_requests_status = match self.as_mut().poll_write_request(cx)? {
            Poll::Ready(Some(())) => return Poll::Ready(Some(Ok(()))),
            Poll::Ready(None) => ReceiverStatus::Closed,
            Poll::Pending => ReceiverStatus::Pending,
        };

        let canceled_requests_status = match self.as_mut().poll_write_cancel(cx)? {
            Poll::Ready(Some(())) => return Poll::Ready(Some(Ok(()))),
            Poll::Ready(None) => ReceiverStatus::Closed,
            Poll::Pending => ReceiverStatus::Pending,
        };

        // Receiving Poll::Ready(None) when polling expired requests never indicates "Closed",
        // because there can temporarily be zero in-flight rquests. Therefore, there is no need to
        // track the status like is done with pending and cancelled requests.
        if let Poll::Ready(Some(_)) = self
            .in_flight_requests()
            .poll_expired(cx, || Err(RpcError::DeadlineExceeded))
        {
            // Expired requests are considered complete; there is no compelling reason to send a
            // cancellation message to the server, since it will have already exhausted its
            // allotted processing time.
            return Poll::Ready(Some(Ok(())));
        }

        match (pending_requests_status, canceled_requests_status) {
            (ReceiverStatus::Closed, ReceiverStatus::Closed) => {
                ready!(self.poll_close(cx)?);
                Poll::Ready(None)
            }
            (ReceiverStatus::Pending, _) | (_, ReceiverStatus::Pending) => {
                // No more messages to process, so flush any messages buffered in the transport.
                ready!(self.poll_flush(cx)?);

                // Even if we fully-flush, we return Pending, because we have no more requests
                // or cancellations right now.
                Poll::Pending
            }
        }
    }

    /// Yields the next pending request, if one is ready to be sent.
    ///
    /// Note that a request will only be yielded if the transport is *ready* to be written to (i.e.
    /// start_send would succeed).
    ///
    /// Side effect: will flush the transport if it is full.
    fn poll_next_request(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<DispatchRequest<Req, Resp>, ChannelError<C::Error>>>> {
        if self.in_flight_requests().len() >= self.config.max_in_flight_requests {
            tracing::info!(
                "At in-flight request capacity ({}/{}).",
                self.in_flight_requests().len(),
                self.config.max_in_flight_requests
            );

            // No need to schedule a wakeup, because timers and responses are responsible
            // for clearing out in-flight requests.
            return Poll::Pending;
        }

        ready!(self.ensure_writeable(cx)?);

        loop {
            match ready!(self.pending_requests_mut().poll_next_unpin(cx)) {
                Some(request) => {
                    if request.response_completion.is_canceled() {
                        let _entered = request.span.enter();
                        tracing::info!("AbortRequest");
                        continue;
                    }

                    return Poll::Ready(Some(Ok(request)));
                }
                None => return Poll::Ready(None),
            }
        }
    }

    /// Yields the next pending cancellation, and, if one is ready, cancels the associated request.
    ///
    /// Note that a request to cancel will only be yielded if the transport is *ready* to be
    /// written to (i.e. start_send would succeed).
    ///
    /// Side effect: will flush the transport if it is full.
    fn poll_next_cancellation(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(context::Context, Span, u64), ChannelError<C::Error>>>> {
        ready!(self.ensure_writeable(cx)?);

        loop {
            match ready!(self.canceled_requests_mut().poll_next_unpin(cx)) {
                Some(request_id) => {
                    if let Some((ctx, span)) = self.in_flight_requests().cancel_request(request_id)
                    {
                        return Poll::Ready(Some(Ok((ctx, span, request_id))));
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }

    /// Returns Ready if writing a message to the transport (i.e. via write_request or
    /// write_cancel) would not fail due to a full buffer. If the transport is not ready to be
    /// written to, flushes it until it is ready.
    fn ensure_writeable<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), ChannelError<C::Error>>>> {
        while self.poll_ready(cx)?.is_pending() {
            ready!(self.poll_flush(cx)?);
        }
        Poll::Ready(Some(Ok(())))
    }

    /// Polls the next request and, if available, writes it to the transport.
    ///
    /// Note that a request will only be polled if the transport is *ready* to be written to (i.e.
    /// start_send would succeed).
    ///
    /// Side effect: will flush the transport if it is full.
    fn poll_write_request<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), ChannelError<C::Error>>>> {
        let DispatchRequest {
            ctx,
            span,
            request_id,
            request,
            response_completion,
        } = match ready!(self.as_mut().poll_next_request(cx)?) {
            Some(dispatch_request) => dispatch_request,
            None => return Poll::Ready(None),
        };
        let _entered = span.enter();
        // poll_next_request only returns Ready if there is room to buffer another request.
        // Therefore, we can call write_request without fear of erroring due to a full
        // buffer.
        let request = ClientMessage::Request(Request {
            id: request_id,
            message: request,
            context: context::Context {
                deadline: ctx.deadline,
                trace_context: ctx.trace_context,
            },
        });
        self.in_flight_requests()
            .insert_request(request_id, ctx, span.clone(), response_completion)
            .expect("Request IDs should be unique");
        match self.start_send(request) {
            Ok(()) => tracing::info!("SendRequest"),
            Err(e) => {
                self.in_flight_requests()
                    .complete_request(request_id, Err(RpcError::Send(Box::new(e))));
            }
        }
        Poll::Ready(Some(Ok(())))
    }

    /// Polls the next cancellation message and, if available, writes it to the transport.
    ///
    /// Note that a request will only be polled if the transport is *ready* to be written to (i.e.
    /// start_send would succeed).
    ///
    /// Side effect: will flush the transport if it is full.
    fn poll_write_cancel<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), ChannelError<C::Error>>>> {
        let (context, span, request_id) = match ready!(self.as_mut().poll_next_cancellation(cx)?) {
            Some(triple) => triple,
            None => return Poll::Ready(None),
        };
        let _entered = span.enter();

        let cancel = ClientMessage::Cancel {
            trace_context: context.trace_context,
            request_id,
        };
        self.start_send(cancel)
            .map_err(|e| ChannelError::Write(Arc::new(e)))?;
        tracing::info!("CancelRequest");
        Poll::Ready(Some(Ok(())))
    }

    /// Sends a server response to the client task that initiated the associated request.
    fn complete(mut self: Pin<&mut Self>, response: Response<Resp>) -> bool {
        if let Some(span) = self.in_flight_requests().complete_request(
            response.request_id,
            response.message.map_err(RpcError::Server),
        ) {
            let _entered = span.enter();
            tracing::info!("ReceiveResponse");
            return true;
        }
        false
    }

    /// Closes the pending requests channel and completes all pending and in-flight requests with
    /// the given error.
    fn shut_down_with_terminal_error(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        e: ChannelError<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Poll<()> {
        self.pending_requests_mut().close();
        for span in self
            .in_flight_requests()
            .complete_all_requests(|| Err(RpcError::Channel(e.clone())))
        {
            let _entered = span.enter();
            tracing::warn!("RpcError::Channel");
        }
        loop {
            match ready!(self.pending_requests_mut().poll_next_unpin(cx)) {
                Some(DispatchRequest {
                    span,
                    response_completion,
                    ..
                }) => {
                    let _entered = span.enter();
                    if response_completion.is_canceled() {
                        tracing::info!("AbortRequest");
                    } else {
                        tracing::warn!("RpcError::Channel");
                        let _ = response_completion.send(Err(RpcError::Channel(e.clone())));
                    }
                }
                None => return Poll::Ready(()),
            }
        }
    }

    fn run<'a>(
        self: &'a mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ChannelError<C::Error>>> {
        loop {
            match (self.as_mut().pump_read(cx)?, self.as_mut().pump_write(cx)?) {
                (Poll::Ready(None), _) => {
                    tracing::info!("Shutdown: read half closed, so shutting down.");
                    return Poll::Ready(Ok(()));
                }
                (read, Poll::Ready(None)) => {
                    if self.in_flight_requests.is_empty() {
                        tracing::info!("Shutdown: write half closed, and no requests in flight.");
                        return Poll::Ready(Ok(()));
                    }
                    tracing::info!(
                        "Shutdown: write half closed, and {} requests in flight.",
                        self.in_flight_requests().len()
                    );
                    match read {
                        Poll::Ready(Some(())) => continue,
                        _ => return Poll::Pending,
                    }
                }
                (Poll::Ready(Some(())), _) | (_, Poll::Ready(Some(()))) => {}
                _ => return Poll::Pending,
            }
        }
    }
}

impl<Req, Resp, C> Future for RequestDispatch<Req, Resp, C>
where
    C: Transport<ClientMessage<Req>, Response<Resp>>,
{
    type Output = Result<(), ChannelError<C::Error>>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ChannelError<C::Error>>> {
        loop {
            if let Some(e) = self.terminal_error_mut() {
                tracing::info!("RpcError::Channel");
                let e: ChannelError<C::Error> = e
                    .clone()
                    .downcast()
                    .expect("Invariant: ChannelError must store a C::Error");
                ready!(self.shut_down_with_terminal_error(cx, e.clone().upcast_error()));
                return Poll::Ready(Err(e));
            }
            let result = ready!(self.run(cx));
            match result {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(e) => *self.terminal_error_mut() = Some(e.upcast_any()),
            }
        }
    }
}

/// A server-bound request sent from a [`Channel`] to request dispatch, which will then manage
/// the lifecycle of the request.
#[derive(Debug)]
struct DispatchRequest<Req, Resp> {
    pub ctx: context::Context,
    pub span: Span,
    pub request_id: u64,
    pub request: Req,
    pub response_completion: oneshot::Sender<Result<Resp, RpcError>>,
}
