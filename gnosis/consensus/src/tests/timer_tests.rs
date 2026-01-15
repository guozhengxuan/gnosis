use crate::timer::Timer;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};

/// Wrapper to make Arc<Mutex<Timer>> awaitable
struct TimerFuture {
    timer: Arc<Mutex<Timer>>,
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut timer = self.timer.try_lock().unwrap();
        Pin::new(&mut *timer).poll(cx)
    }
}

/// Test case 1: Timer completes normally without any reset
#[tokio::test]
async fn test_timer_completes_normally() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);

    // Thread A: Reset timer to 100ms
    {
        let mut t = timer.lock().await;
        t.reset(Some(100));
    }

    // Thread B: Loop with select
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    break count;
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    assert_eq!(msg, "tick");
                }
            }
        }
    });

    // Send some messages while timer is running
    tx.send("tick".to_string()).await.unwrap();
    tx.send("tick".to_string()).await.unwrap();

    let final_count = handle.await.unwrap();
    assert!(final_count >= 2, "Should have processed at least 2 messages");
}

/// Test case 2: Timer is reset to None while B is waiting - loop should never end naturally
#[tokio::test]
async fn test_timer_reset_to_none_keeps_looping() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);

    // Thread A: Reset timer to 200ms initially
    {
        let mut t = timer.lock().await;
        t.reset(Some(200));
    }

    // Thread B: Loop with select
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed - this should NOT happen
                    return (count, true);
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    assert_eq!(msg, "tick");
                }
                _ = stop_rx.recv() => {
                    // Stop signal received
                    return (count, false);
                }
            }
        }
    });

    // Send a message
    tx.send("tick".to_string()).await.unwrap();
    sleep(Duration::from_millis(10)).await; // Let it process

    // Thread A: Reset timer to None after 50ms (before original 200ms timeout)
    sleep(Duration::from_millis(50)).await;
    {
        let mut t = timer.lock().await;
        t.reset(None);
    }

    // Send more messages
    tx.send("tick".to_string()).await.unwrap();
    sleep(Duration::from_millis(10)).await; // Let it process
    tx.send("tick".to_string()).await.unwrap();
    sleep(Duration::from_millis(10)).await; // Let it process

    // Wait a bit to ensure timer would have completed if not reset to None
    sleep(Duration::from_millis(200)).await;

    // Send more messages to verify loop is still running
    tx.send("tick".to_string()).await.unwrap();
    sleep(Duration::from_millis(10)).await; // Let it process

    // Stop the loop
    stop_tx.send(()).await.unwrap();

    let (final_count, timer_completed) = handle.await.unwrap();
    assert!(!timer_completed, "Timer should NOT have completed");
    assert!(final_count >= 4, "Should have processed at least 4 messages");
}

/// Test case 3: Timer is reset with a new Some duration while B is waiting
#[tokio::test]
async fn test_timer_reset_to_new_duration() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);

    // Thread A: Reset timer to 300ms initially
    {
        let mut t = timer.lock().await;
        t.reset(Some(300));
    }

    // Thread B: Loop with select
    let timer_clone = timer.clone();
    let start = std::time::Instant::now();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    break (count, start.elapsed().as_millis());
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    assert_eq!(msg, "tick");
                }
            }
        }
    });

    // Send a message
    tx.send("tick".to_string()).await.unwrap();

    // Thread A: Reset timer to 100ms after 50ms (restarts the timer)
    sleep(Duration::from_millis(50)).await;
    {
        let mut t = timer.lock().await;
        t.reset(Some(100));
    }

    // Send more messages
    tx.send("tick".to_string()).await.unwrap();

    let (final_count, elapsed) = handle.await.unwrap();
    assert!(final_count >= 2, "Should have processed at least 2 messages");
    // Timer should complete around 150ms (50ms + 100ms), not 300ms
    assert!(elapsed >= 130 && elapsed < 250, "Timer should complete after ~150ms, got {}ms", elapsed);
}

/// Test case 4: Timer starts as None, then reset to Some
#[tokio::test]
async fn test_timer_none_to_some() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);

    // Timer is None initially

    // Thread B: Loop with select
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    break count;
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    assert_eq!(msg, "tick");
                }
            }
        }
    });

    // Send messages while timer is None
    tx.send("tick".to_string()).await.unwrap();
    tx.send("tick".to_string()).await.unwrap();

    sleep(Duration::from_millis(50)).await;

    // Thread A: Reset timer to Some(100ms)
    {
        let mut t = timer.lock().await;
        t.reset(Some(100));
    }

    // Send more messages
    tx.send("tick".to_string()).await.unwrap();

    let final_count = handle.await.unwrap();
    assert!(final_count >= 3, "Should have processed at least 3 messages");
}

/// Test case 5: Timer is Some, reset to None, then reset back to Some
#[tokio::test]
async fn test_timer_some_none_some() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);

    // Thread A: Reset timer to 500ms initially
    {
        let mut t = timer.lock().await;
        t.reset(Some(500));
    }

    // Thread B: Loop with select
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    break count;
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    assert_eq!(msg, "tick");
                }
            }
        }
    });

    // Send a message
    tx.send("tick".to_string()).await.unwrap();

    // Thread A: Reset timer to None after 50ms
    sleep(Duration::from_millis(50)).await;
    {
        let mut t = timer.lock().await;
        t.reset(None);
    }

    // Send more messages
    tx.send("tick".to_string()).await.unwrap();
    tx.send("tick".to_string()).await.unwrap();

    // Thread A: Reset timer back to Some(100ms) after another 50ms
    sleep(Duration::from_millis(50)).await;
    {
        let mut t = timer.lock().await;
        t.reset(Some(100));
    }

    // Send more messages
    tx.send("tick".to_string()).await.unwrap();

    let final_count = handle.await.unwrap();
    assert!(final_count >= 4, "Should have processed at least 4 messages");
}

/// Test case 6: Complex scenario with multiple resets and channel activity
#[tokio::test]
async fn test_timer_complex_scenario() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(10);
    let (result_tx, mut result_rx) = mpsc::channel::<(usize, bool)>(1);

    // Thread A: Reset timer to 150ms initially
    {
        let mut t = timer.lock().await;
        t.reset(Some(150));
    }

    // Thread B: Loop with select - processing messages and waiting for timer
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        let mut timer_completed = false;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    timer_completed = true;
                    break;
                }
                Some(msg) = rx.recv() => {
                    count += 1;
                    if msg == "stop" {
                        break;
                    }
                }
            }
        }
        result_tx.send((count, timer_completed)).await.unwrap();
    });

    // Simulate active message processing with timer manipulations
    for i in 0..5 {
        tx.send(format!("msg_{}", i)).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }

    // Reset to None (disable timer)
    {
        let mut t = timer.lock().await;
        t.reset(None);
    }

    // More messages while timer is disabled
    for i in 5..10 {
        tx.send(format!("msg_{}", i)).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }

    // Reset to Some again (re-enable timer with short duration)
    {
        let mut t = timer.lock().await;
        t.reset(Some(50));
    }

    // Wait for timer to complete
    handle.await.unwrap();

    let (final_count, timer_completed) = result_rx.recv().await.unwrap();
    assert!(timer_completed, "Timer should have completed");
    assert!(final_count >= 10, "Should have processed at least 10 messages, got {}", final_count);
}

/// Test case 7: Rapid resets - stress test
#[tokio::test]
async fn test_timer_rapid_resets() {
    let timer = Arc::new(Mutex::new(Timer::new()));
    let (tx, mut rx) = mpsc::channel::<String>(100);
    let (_stop_tx, mut stop_rx) = mpsc::channel::<()>(1);

    // Thread A: Reset timer to 100ms initially
    {
        let mut t = timer.lock().await;
        t.reset(Some(100));
    }

    // Thread B: Loop processing messages
    let timer_clone = timer.clone();
    let handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::select! {
                _ = TimerFuture { timer: timer_clone.clone() } => {
                    // Timer completed
                    return (count, true);
                }
                Some(_msg) = rx.recv() => {
                    count += 1;
                }
                _ = stop_rx.recv() => {
                    return (count, false);
                }
            }
        }
    });

    // Thread A: Rapidly reset timer multiple times
    let timer_clone = timer.clone();
    let reset_handle = tokio::spawn(async move {
        for i in 0..10 {
            sleep(Duration::from_millis(10)).await;
            let mut t = timer_clone.lock().await;
            if i % 3 == 0 {
                t.reset(None); // Disable
            } else if i % 3 == 1 {
                t.reset(Some(50)); // Short timeout
            } else {
                t.reset(Some(200)); // Long timeout
            }
        }
        // Finally set to complete quickly
        sleep(Duration::from_millis(10)).await;
        let mut t = timer_clone.lock().await;
        t.reset(Some(50));
    });

    // Keep sending messages
    let tx_handle = tokio::spawn(async move {
        for i in 0..50 {
            sleep(Duration::from_millis(5)).await;
            if tx.send(format!("msg_{}", i)).await.is_err() {
                break;
            }
        }
    });

    reset_handle.await.unwrap();
    tx_handle.await.unwrap();

    let (final_count, timer_completed) = handle.await.unwrap();
    assert!(timer_completed, "Timer should have completed");
    assert!(final_count > 0, "Should have processed some messages");
}
