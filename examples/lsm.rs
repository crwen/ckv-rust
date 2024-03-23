use anyhow::Result;
use ckv::{lsm::Lsm, write_batch::WriteBatch, Options};

fn main() -> Result<()> {
    let opt = Options::default_opt()
        .work_dir("work_dir/lsm")
        .mem_size(1 << 12)
        .kv_separate_threshold(4);
    let lsm = Lsm::open(opt);
    lsm.put(b"key1", b"val1")?;
    lsm.get(b"key1")?;

    let mut wb = WriteBatch::default();
    wb.put(b"key2", b"val2");
    wb.put(b"key3", b"val3");
    wb.delete(b"key1");
    lsm.write_batch(&wb)?;

    lsm.get(b"key1")?;
    Ok(())
}
