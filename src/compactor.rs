use std::{
    sync::{
        mpsc::{Receiver, RecvTimeoutError},
        Arc,
    },
    time::Duration,
};

use crate::lsm::LsmInner;

pub struct Compactor {
    handle: Receiver<()>,
    lsm_inner: Arc<LsmInner>,
}

impl Compactor {
    pub fn new(handle: Receiver<()>, lsm_inner: Arc<LsmInner>) -> Self {
        Self { handle, lsm_inner }
    }

    pub fn run_compactor(&self) {
        loop {
            match self.handle.recv_timeout(Duration::from_secs(3)) {
                Ok(_) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
            if self.lsm_inner.imms_sz() > 0 {
                self.lsm_inner.compact_mem_table();
            } else {
                // compact sst
            }
        }
    }
}
