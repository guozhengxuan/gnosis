use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::time::{sleep, Duration, Instant, Sleep};

#[cfg(test)]
#[path = "tests/timer_tests.rs"]
pub mod timer_tests;

pub struct Timer {
    sleep: Option<Pin<Box<Sleep>>>,
    waker: Option<Waker>,
}

impl Timer {
    pub fn new() -> Self {
        Self { sleep: None, waker: None }
    }

    pub fn reset(&mut self, duration: Option<u64>) {
        match duration {
            Some(d) => {
                if let Some(sleep) = self.sleep.as_mut() {
                    sleep.as_mut().reset(Instant::now() + Duration::from_millis(d));
                } else {
                    self.sleep = Some(Box::pin(sleep(Duration::from_millis(d))));
                }
            }
            None => {
                self.sleep = None;
            }
        }
        // Wake any waiting task so it re-polls and discovers the updated sleep.
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.waker = Some(cx.waker().clone()); // Store waker
        match self.sleep.as_mut() {
            Some(sleep) => sleep.as_mut().poll(cx),
            None => Poll::Pending,
        }
    }
}