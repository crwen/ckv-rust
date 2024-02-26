use crate::{lsm::Lsm, write_batch::WriteBatch, Options};

fn clear_dir(work_dir: &str) {
    if std::fs::metadata(work_dir).is_ok() {
        std::fs::remove_dir_all(work_dir).unwrap()
    };
}

#[test]
fn test_write_batch() {
    let opt = Options::default_opt()
        .work_dir("work_dir/batch")
        .mem_size(1 << 12)
        .kv_separate_threshold(4);
    clear_dir(&opt.work_dir);

    let mut batch = WriteBatch::default();
    for i in 0..10 {
        let n = i as u32;
        batch.put(&n.to_be_bytes(), &n.to_be_bytes());
    }
    for i in 0..10 {
        if i % 2 == 0 {
            let n = i as u32;
            batch.delete(&n.to_be_bytes());
        }
    }

    let lsm = Lsm::open(opt.clone());
    let result = lsm.write_batch(&batch);
    assert!(result.is_ok());

    for i in 0..10 {
        let n = i as u32;
        let result = lsm.get(&n.to_be_bytes());
        assert!(result.is_ok());
        let value = result.unwrap();
        if i % 2 == 0 {
            assert_eq!(value, None);
        } else {
            assert_ne!(value, None);
            assert_eq!(value.unwrap(), &n.to_be_bytes());
        }
    }

    clear_dir(&opt.work_dir);
}

#[test]
fn test_write_batch_recover_mem() {
    let opt = Options::default_opt()
        .work_dir("work_dir/batch_recv")
        .mem_size(1 << 12)
        .kv_separate_threshold(4);
    clear_dir(&opt.work_dir);

    let mut batch = WriteBatch::default();
    for i in 0..10 {
        let n = i as u32;
        batch.put(&n.to_be_bytes(), &n.to_be_bytes());
    }

    {
        let lsm = Lsm::open(opt.clone());
        let result = lsm.write_batch(&batch);
        assert!(result.is_ok());
    }

    let lsm = Lsm::open(opt.clone());

    for i in 0..10 {
        let n = i as u32;
        let result = lsm.get(&n.to_be_bytes());
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_ne!(value, None);
        assert_eq!(value.unwrap(), &n.to_be_bytes());
    }

    clear_dir(&opt.work_dir);
}
