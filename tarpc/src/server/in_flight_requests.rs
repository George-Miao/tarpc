use crate::util::{Compact, TimeUntil};
use fnv::FnvHashMap;
use futures::future::{AbortHandle, AbortRegistration};
use std::{
    collections::hash_map,
    task::{Context, Poll},
    time::Instant,
};
use tracing::Span;

/// A data structure that tracks in-flight requests. It aborts requests,
/// either on demand or when a request deadline expires.
#[derive(Debug, Default)]
pub struct InFlightRequests {
    request_data: FnvHashMap<u64, RequestData>,
}

/// Data needed to clean up a single in-flight request.
#[derive(Debug)]
struct RequestData {
    /// Aborts the response handler for the associated request.
    abort_handle: AbortHandle,
    /// The client span.
    span: Span,
}

/// An error returned when a request attempted to start with the same ID as a request already
/// in flight.
#[derive(Debug)]
pub struct AlreadyExistsError;

impl InFlightRequests {
    /// Returns the number of in-flight requests.
    pub fn len(&self) -> usize {
        self.request_data.len()
    }

    /// Starts a request, unless a request with the same ID is already in flight.
    pub fn start_request(
        &mut self,
        request_id: u64,
        deadline: Instant,
        span: Span,
    ) -> Result<AbortRegistration, AlreadyExistsError> {
        match self.request_data.entry(request_id) {
            hash_map::Entry::Vacant(vacant) => {
                let timeout = deadline.time_until();
                let (abort_handle, abort_registration) = AbortHandle::new_pair();
                vacant.insert(RequestData { abort_handle, span });
                Ok(abort_registration)
            }
            hash_map::Entry::Occupied(_) => Err(AlreadyExistsError),
        }
    }

    /// Cancels an in-flight request. Returns true iff the request was found.
    pub fn cancel_request(&mut self, request_id: u64) -> bool {
        if let Some(RequestData { span, abort_handle }) = self.request_data.remove(&request_id) {
            let _entered = span.enter();
            self.request_data.compact(0.1);
            abort_handle.abort();
            tracing::info!("ReceiveCancel");
            true
        } else {
            false
        }
    }

    /// Removes a request without aborting. Returns true iff the request was found.
    /// This method should be used when a response is being sent.
    pub fn remove_request(&mut self, request_id: u64) -> Option<Span> {
        if let Some(request_data) = self.request_data.remove(&request_id) {
            self.request_data.compact(0.1);
            Some(request_data.span)
        } else {
            None
        }
    }

    /// Yields a request that has expired, aborting any ongoing processing of that request.
    pub fn poll_expired(&mut self, cx: &mut Context) -> Poll<Option<u64>> {
        if self.request_data.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// When InFlightRequests is dropped, any outstanding requests are aborted.
impl Drop for InFlightRequests {
    fn drop(&mut self) {
        self.request_data
            .values()
            .for_each(|request_data| request_data.abort_handle.abort())
    }
}
