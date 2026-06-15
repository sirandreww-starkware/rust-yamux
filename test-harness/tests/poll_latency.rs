// Copyright (c) 2024 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

//! Demonstrates that a single `Connection::poll_next_inbound` call grows costly
//! as the inbound burst it has to drain grows.
//!
//! `poll_next_inbound` pumps the whole connection: each call reads the socket,
//! decodes every yamux frame currently available, allocates a body buffer per
//! data frame and dispatches it to the stream. So draining a 256 KiB message
//! (~16 frames at the 16 KiB default split size) does ~16x the per-frame work of
//! a 16 KiB message (1 frame) — in one poll. That violates the cooperative
//! scheduling guideline (a poll should return within ~50 µs) and is the root
//! cause of long `Connection::poll` times under load.
//!
//! This test times every `poll_next_inbound` call while draining a single
//! message, for a few message sizes, and asserts that the worst single poll
//! scales with burst size. Run with `--nocapture` to see the numbers:
//!
//!     cargo test -p test-harness --test poll_latency -- --nocapture

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, Either};
use futures::stream::FuturesUnordered;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, FutureExt, StreamExt};
use futures_ringbuf::Endpoint;
use tokio::runtime::Builder;
use yamux::{Config, Connection, ConnectionError, Mode};

const KIB: usize = 1024;

/// In-memory duplex buffer per direction. Must exceed the largest message (256
/// KiB) plus framing + window-update room so the whole burst can be buffered
/// before the server drains it.
const PIPE_BUF: usize = 1024 * KIB;

/// Server-side driver that records the wall-clock duration of every
/// `poll_next_inbound` call (the connection pump that decodes + dispatches
/// frames) and drains the inbound stream until `expected` bytes have arrived.
/// Resolves to `(per-poll durations, bytes drained)`.
///
/// The per-stream reads (cheap copies from the already-decoded buffer) run in a
/// worker future, *outside* the timed region, so each recorded duration is
/// attributable to frame decoding/dispatch in the pump.
struct TimedDrainServer<T> {
    connection: Connection<T>,
    expected: usize,
    workers: FuturesUnordered<BoxFuture<'static, Result<usize, ConnectionError>>>,
    poll_durations: Vec<Duration>,
    bytes_drained: usize,
}

impl<T> TimedDrainServer<T> {
    fn new(connection: Connection<T>, expected: usize) -> Self {
        Self {
            connection,
            expected,
            workers: FuturesUnordered::default(),
            poll_durations: Vec::new(),
            bytes_drained: 0,
        }
    }
}

impl<T> Future for TimedDrainServer<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = (Vec<Duration>, usize);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.workers.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(n))) => {
                    this.bytes_drained += n;
                    if this.bytes_drained >= this.expected {
                        return Poll::Ready((
                            std::mem::take(&mut this.poll_durations),
                            this.bytes_drained,
                        ));
                    }
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    eprintln!("drain worker failed: {e}");
                    continue;
                }
                Poll::Ready(None) | Poll::Pending => {}
            }

            // The timed call: one poll drains all frames currently available.
            let expected = this.expected;
            let start = Instant::now();
            let polled = this.connection.poll_next_inbound(cx);
            this.poll_durations.push(start.elapsed());

            match polled {
                Poll::Ready(Some(Ok(mut stream))) => {
                    this.workers.push(
                        async move {
                            let mut buf = [0u8; 64 * KIB];
                            let mut total = 0;
                            while total < expected {
                                let n = stream.read(&mut buf).await?;
                                if n == 0 {
                                    break;
                                }
                                total += n;
                            }
                            Ok::<usize, ConnectionError>(total)
                        }
                        .boxed(),
                    );
                    continue;
                }
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                    // Connection ended before we drained `expected`; report what we have.
                    return Poll::Ready((
                        std::mem::take(&mut this.poll_durations),
                        this.bytes_drained,
                    ));
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

/// Client-side driver: opens one outbound stream, writes+flushes `msg` (setting
/// `wrote` once the whole message has been pushed onto the wire), then keeps the
/// connection alive and pumped — so the server can drain it and have its window
/// updates accepted — without ever closing it. Never resolves on its own; it is
/// dropped once the server side finishes (see `drain_one_message`).
struct ClientPump<T> {
    connection: Connection<T>,
    msg: Vec<u8>,
    wrote: Arc<AtomicBool>,
    worker: Option<BoxFuture<'static, ()>>,
    started: bool,
}

impl<T> ClientPump<T> {
    fn new(connection: Connection<T>, msg: Vec<u8>, wrote: Arc<AtomicBool>) -> Self {
        Self {
            connection,
            msg,
            wrote,
            worker: None,
            started: false,
        }
    }
}

impl<T> Future for ClientPump<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        loop {
            if !this.started {
                match this.connection.poll_new_outbound(cx) {
                    Poll::Ready(Ok(mut stream)) => {
                        let msg = std::mem::take(&mut this.msg);
                        let wrote = this.wrote.clone();
                        this.worker = Some(
                            async move {
                                stream.write_all(&msg).await.unwrap();
                                stream.flush().await.unwrap();
                                wrote.store(true, Ordering::Relaxed);
                                // Hold the stream open (no FIN) and idle forever.
                                futures::future::pending::<()>().await;
                            }
                            .boxed(),
                        );
                        this.started = true;
                        continue;
                    }
                    Poll::Ready(Err(_)) => this.started = true,
                    Poll::Pending => {}
                }
            }

            if let Some(worker) = this.worker.as_mut() {
                let _ = worker.poll_unpin(cx); // drive the write; then it parks
            }

            // Pump the connection so queued frames go out and the peer's window
            // updates are consumed. Never close.
            match this.connection.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(_))) => continue,
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => return Poll::Pending,
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

/// Send a single `msg_len`-byte message on one stream and drain it on the other
/// side, returning the per-`poll_next_inbound` durations and bytes drained.
///
/// Uses an in-memory, unbounded, zero-delay duplex so reads return immediately
/// and a poll is pure decode/dispatch CPU (wall-clock ≈ CPU time). `msg_len`
/// must be `<= DEFAULT_CREDIT` (256 KiB) so the message fits the receive window
/// and arrives as one burst without window-update round-trips.
fn drain_one_message(msg_len: usize) -> (Vec<Duration>, usize) {
    let rt = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime");

    rt.block_on(async move {
        let (server_io, client_io) = Endpoint::pair(PIPE_BUF, PIPE_BUF);
        let server = Connection::new(server_io, Config::default(), Mode::Server);
        let client = Connection::new(client_io, Config::default(), Mode::Client);

        let wrote = Arc::new(AtomicBool::new(false));
        let mut client_fut = ClientPump::new(client, vec![0x42u8; msg_len], wrote.clone());

        // Phase 1: drive ONLY the client until the whole message is on the wire,
        // so the entire burst is buffered in the pipe before the server looks.
        futures::future::poll_fn(|cx| {
            let _ = client_fut.poll_unpin(cx);
            if wrote.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;

        // Phase 2: time the server draining the now-buffered burst. The client
        // keeps running (alive, not dropped) so its endpoint stays connected and
        // the server's window updates are accepted; it's dropped when the server
        // resolves.
        let server_fut = TimedDrainServer::new(server, msg_len);
        match futures::future::select(server_fut, client_fut).await {
            Either::Left((out, _client)) => out,
            Either::Right(((), _server)) => unreachable!("client never resolves"),
        }
    })
}

#[test]
fn poll_time_scales_with_inbound_burst() {
    let _ = env_logger::try_init();

    // Warm up allocator / code paths so the first measured size isn't inflated
    // by cold-start cost.
    let _ = drain_one_message(64 * KIB);

    let sizes = [16 * KIB, 64 * KIB, 256 * KIB];
    let mut max_polls = Vec::new();

    println!();
    for &size in &sizes {
        let (durations, bytes) = drain_one_message(size);
        assert_eq!(
            bytes, size,
            "server must drain the whole {size}-byte message"
        );

        let max = durations.iter().copied().max().expect("at least one poll");
        let total: Duration = durations.iter().sum();
        println!(
            "msg = {:>3} KiB | poll_next_inbound calls = {:<4} | worst single poll = {:>12.3?} | total = {:.3?}",
            size / KIB,
            durations.len(),
            max,
            total,
        );
        max_polls.push(max);
    }

    let max_16k = max_polls[0];
    let max_256k = max_polls[2];
    const GUIDELINE: Duration = Duration::from_micros(50);
    println!("\nworst 256 KiB poll = {max_256k:.3?}  (cooperative-scheduling guideline ≈ 50 µs)\n");

    // The problem itself: draining a single, ordinary 256 KiB message takes one
    // poll far longer than the ~50 µs a cooperative poll should run — so it
    // monopolises the executor. (Run in debug, the default; release is faster
    // but still well over the guideline.)
    assert!(
        max_256k > GUIDELINE,
        "expected the worst 256 KiB poll ({max_256k:?}) to exceed the ~50 µs cooperative \
         guideline — that long poll is the problem"
    );

    // And it scales with burst size: the worst poll grows with the number of
    // frames drained. The 16 KiB worst poll carries a fixed SYN/stream-setup
    // cost that doesn't scale, so use a conservative 3x floor (observed ~3.5-5x)
    // rather than the ~16x frame ratio.
    assert!(
        max_256k >= max_16k * 3,
        "expected the worst 256 KiB poll ({max_256k:?}) to be >= 3x the worst 16 KiB poll \
         ({max_16k:?}); poll time did not scale with inbound burst size"
    );
}
