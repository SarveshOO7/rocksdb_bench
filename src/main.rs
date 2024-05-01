fn main() {
    println!("Hello, world!");
}

use rand::distributions::Bernoulli;
use rand::{distributions::Distribution, Rng};
use rocksdb::{DBPath, DBWithThreadMode, MultiThreaded, DB};
use std::sync::Arc;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};
use tracing::trace;

use tokio::task::LocalSet;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

const PAGE_SIZE: usize = 4 * 1024;
const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

#[test]
fn bench() {
    const THREADS: usize = 16;
    const TASKS: usize = 1; // tasks per thread
    const OPERATIONS: usize = 1 << 20;

    const THREAD_OPERATIONS: usize = OPERATIONS / THREADS;
    const ITERATIONS: usize = THREAD_OPERATIONS / TASKS; // iterations per task

    const FRAMES: usize = GIGABYTE_PAGES;
    const DISK_PAGES: usize = 32 * GIGABYTE_PAGES;

    let coin = Bernoulli::new(0.2).unwrap();

    println!("Operations: {OPERATIONS}");

    let zeros: [u8; 4096] = [0; 4096];

    let db =
        DBWithThreadMode::<MultiThreaded>::open_default("_rust_rocksdb_multithreadtest").unwrap();
    let db = Arc::new(db);

    // Spawn all threads
    for id in 0..DISK_PAGES {
        let key: [u8; 8] = id.to_le_bytes();
        let _ = db.put(key, zeros);
        println!("id {}", id);
    }

    println!("Added all the pages");

    // Spawn all threads
    thread::scope(|s| {
        for thread in 0..THREADS {
            let local_db = db.clone();
            s.spawn(move || {
                let mut rng = rand::thread_rng();
                for iteration in 0..ITERATIONS {
                    let id = rng.gen_range(0..DISK_PAGES) as u64;
                    let key: [u8; 8] = id.to_le_bytes();

                    if coin.sample(&mut rng) {
                        local_db.put(key, zeros);
                    } else {
                        let slice = local_db.get(key);
                        std::hint::black_box(slice);
                    }

                    COUNTER.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        s.spawn(|| {
            let duration = std::time::Duration::from_secs(1);
            let mut prev = 0;
            while COUNTER.load(Ordering::SeqCst) < THREADS * TASKS * ITERATIONS {
                println!("Counter is at: {:?}", COUNTER.load(Ordering::SeqCst) - prev);
                prev = COUNTER.load(Ordering::SeqCst);
                std::thread::sleep(duration);
            }
        });
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), THREADS * TASKS * ITERATIONS);
}
