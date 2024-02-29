use ckv::lsm::Lsm;
use criterion::{criterion_group, criterion_main, Criterion};

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        // This is the 32-bit variant of Xorshift.
        //
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        // This is a fast alternative to `x % n`.
        //
        // Author: Daniel Lemire
        // Source: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn random_data() -> Vec<u8> {
    const SIZE: u32 = 65536;
    // const REPEAT: u32 = 18;
    const REPEAT: u32 = 1;
    let repeat = random(REPEAT);
    let mut data = vec![];
    for _ in 0..repeat {
        data.append(&mut random(SIZE).to_be_bytes().to_vec());
    }
    data
}

fn ckv_monotonic_crud(c: &mut Criterion) {
    let opt = ckv::Options::default_opt()
        .work_dir("work_dir/bench/monotonic")
        .mem_size(1 << 20)
        .cache_size(1 << 24)
        .kv_separate_threshold(64);
    if std::fs::metadata(&opt.work_dir).is_ok() {
        std::fs::remove_dir_all(&opt.work_dir).unwrap()
    };

    let lsm = Lsm::open(opt);

    c.bench_function("monotonic inserts", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            lsm.put(&count.to_be_bytes(), &count.to_be_bytes()).unwrap();
        })
    });
    c.bench_function("monotonic gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            lsm.get(&count.to_be_bytes()).unwrap();
        })
    });

    c.bench_function("monotonic deletes", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            lsm.delete(&count.to_be_bytes()).unwrap();
        })
    });
}

fn ckv_random_crud(c: &mut Criterion) {
    let opt = ckv::Options::default_opt()
        .work_dir("work_dir/bench/random")
        .mem_size(1 << 20)
        .cache_size(1 << 24)
        .kv_separate_threshold(64);
    if std::fs::metadata(&opt.work_dir).is_ok() {
        std::fs::remove_dir_all(&opt.work_dir).unwrap()
    };
    let lsm = Lsm::open(opt);
    c.bench_function("random inserts", |b| {
        b.iter(|| {
            lsm.put(&random_data(), &random_data()).unwrap();
        })
    });
    c.bench_function("random gets", |b| {
        b.iter(|| {
            lsm.get(&random_data()).unwrap();
        })
    });
    c.bench_function("random deletes", |b| {
        b.iter(|| {
            lsm.delete(&random_data()).unwrap();
        })
    });
}

fn ckv_empty_opens(c: &mut Criterion) {
    let opt = ckv::Options::default_opt().work_dir("work_dir/bench/empty_opens");
    let _ = std::fs::remove_dir_all(&opt.work_dir);
    c.bench_function("empty_opens", |b| {
        b.iter(|| {
            Lsm::open(opt.clone());
        })
    });
    let _ = std::fs::remove_dir_all("work_dir/bench/empty_opens");
}

criterion_group!(
    benches,
    // ckv_bulk_load,
    ckv_monotonic_crud,
    ckv_random_crud,
    ckv_empty_opens
);
criterion_main!(benches);
