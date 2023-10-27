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
    const REPEAT: u32 = 18;
    let repeat = random(REPEAT);
    let mut data = vec![];
    for _ in 0..repeat {
        data.append(&mut random(SIZE).to_be_bytes().to_vec());
    }
    data
}

fn set_benchmark(c: &mut Criterion) {
    let opt = ckv::Options::default_opt()
        .work_dir("work_dir/bench")
        .mem_size(1 << 20)
        .cache_size(1 << 24)
        .kv_separate_threshold(64);
    if std::fs::metadata(&opt.work_dir).is_ok() {
        std::fs::remove_dir_all(&opt.work_dir).unwrap()
    };
    let lsm = Lsm::open(opt);
    c.bench_function("set", |b| {
        b.iter(|| {
            // lsm.put(&random(SIZE).to_be_bytes(), &random(SIZE).to_be_bytes())
            //     .unwrap();
            lsm.put(&random_data(), &random_data()).unwrap();
        })
    });
    c.bench_function("get", |b| {
        b.iter(|| {
            lsm.get(&random_data()).unwrap();
        })
    });
}

// fn criterion_benchmark(c: &mut Criterion) {
//     c.bench_function("fib 20", |b| b.iter(|| fib(black_box(20))));
// }

criterion_group!(benches, set_benchmark);
criterion_main!(benches);
