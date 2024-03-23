

A LSM based Key/Value stroage engine.

- [x] MemTable
- [x] SSTable
- [x] Log
- [x] Compaction
- [x] Bloom Filter
- [x] Cache
- [x] Separating keys from values 
- [x] Write batch

### Usage

```rust
use anyhow::Result;
use ckv::{lsm::Lsm, write_batch::WriteBatch, Options};

fn main() -> Result<()> {
    let opt = Options::default_opt()
        .work_dir("work_dir/lsm")
        .mem_size(1 << 12)
        .kv_separate_threshold(4);

    let lsm = Lsm::open(opt);
    // set a key value
    lsm.put(b"key1", b"val1")?;
    // get value
    lsm.get(b"key1")?;

    // operation by WriteBatch
    let mut wb = WriteBatch::default();
    wb.put(b"key2", b"val2");
    wb.put(b"key3", b"val3");
    wb.delete(b"key1");
    lsm.write_batch(&wb)?;

    lsm.get(b"key1")?;
    Ok(())
}
```


reference
- [LevelDB](https://github.com/google/leveldb)
