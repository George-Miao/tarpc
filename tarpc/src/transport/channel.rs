// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Transports backed by in-memory channels.

use futures::channel::mpsc;
use futures::{task::*, Sink, Stream, StreamExt};
use pin_project::pin_project;
use std::{error::Error, pin::Pin};

/// Errors that occur in the sending or receiving of messages over a channel.
#[derive(thiserror::Error, Debug)]
pub enum ChannelError {
    /// An error occurred readying to send into the channel.
    #[error("an error occurred readying to send into the channel")]
    Ready(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// An error occurred sending into the channel.
    #[error("an error occurred sending into the channel")]
    Send(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// An error occurred receiving from the channel.
    #[error("an error occurred receiving from the channel")]
    Receive(#[source] Box<dyn Error + Send + Sync + 'static>),
}

/// Returns two unbounded channel peers. Each [`Stream`] yields items sent through the other's
/// [`Sink`].
pub fn unbounded<SinkItem, Item>() -> (
    UnboundedChannel<SinkItem, Item>,
    UnboundedChannel<Item, SinkItem>,
) {
    let (tx1, rx2) = mpsc::unbounded();
    let (tx2, rx1) = mpsc::unbounded();
    (
        UnboundedChannel { tx: tx1, rx: rx1 },
        UnboundedChannel { tx: tx2, rx: rx2 },
    )
}

/// A bi-directional channel backed by an [`UnboundedSender`](mpsc::UnboundedSender)
/// and [`UnboundedReceiver`](mpsc::UnboundedReceiver).
#[derive(Debug)]
pub struct UnboundedChannel<Item, SinkItem> {
    rx: mpsc::UnboundedReceiver<Item>,
    tx: mpsc::UnboundedSender<SinkItem>,
}

impl<Item, SinkItem> Stream for UnboundedChannel<Item, SinkItem> {
    type Item = Result<Item, ChannelError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Item, ChannelError>>> {
        self.rx
            .poll_next_unpin(cx)
            .map(|option| option.map(Ok))
            .map_err(ChannelError::Receive)
    }
}

const CLOSED_MESSAGE: &str = "the channel is closed and cannot accept new items for sending";

impl<Item, SinkItem> Sink<SinkItem> for UnboundedChannel<Item, SinkItem> {
    type Error = ChannelError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(if self.tx.is_closed() {
            Err(ChannelError::Ready(CLOSED_MESSAGE.into()))
        } else {
            Ok(())
        })
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
        self.tx
            .unbounded_send(item)
            .map_err(|_| ChannelError::Send(CLOSED_MESSAGE.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedSender requires no flushing.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedSender can't initiate closure.
        Poll::Ready(Ok(()))
    }
}

/// Returns two channel peers with buffer equal to `capacity`. Each [`Stream`] yields items sent
/// through the other's [`Sink`].
pub fn bounded<SinkItem, Item>(
    capacity: usize,
) -> (Channel<SinkItem, Item>, Channel<Item, SinkItem>) {
    let (tx1, rx2) = futures::channel::mpsc::channel(capacity);
    let (tx2, rx1) = futures::channel::mpsc::channel(capacity);
    (Channel { tx: tx1, rx: rx1 }, Channel { tx: tx2, rx: rx2 })
}

/// A bi-directional channel backed by a [`Sender`](futures::channel::mpsc::Sender)
/// and [`Receiver`](futures::channel::mpsc::Receiver).
#[pin_project]
#[derive(Debug)]
pub struct Channel<Item, SinkItem> {
    #[pin]
    rx: futures::channel::mpsc::Receiver<Item>,
    #[pin]
    tx: futures::channel::mpsc::Sender<SinkItem>,
}

impl<Item, SinkItem> Stream for Channel<Item, SinkItem> {
    type Item = Result<Item, ChannelError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Item, ChannelError>>> {
        self.project()
            .rx
            .poll_next(cx)
            .map(|option| option.map(Ok))
            .map_err(ChannelError::Receive)
    }
}

impl<Item, SinkItem> Sink<SinkItem> for Channel<Item, SinkItem> {
    type Error = ChannelError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .tx
            .poll_ready(cx)
            .map_err(|e| ChannelError::Ready(Box::new(e)))
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
        self.project()
            .tx
            .start_send(item)
            .map_err(|e| ChannelError::Send(Box::new(e)))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .tx
            .poll_flush(cx)
            .map_err(|e| ChannelError::Send(Box::new(e)))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .tx
            .poll_close(cx)
            .map_err(|e| ChannelError::Send(Box::new(e)))
    }
}
