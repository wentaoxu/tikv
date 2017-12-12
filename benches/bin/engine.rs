use std::sync::mpsc::{channel, Sender};
use tikv::storage::{new_local_engine, Config, Storage, ALL_CFS};
use tempdir::TempDir;

fn bench_get() {
    let dir = TempDir::new("engine_bench").unwrap();
    let e = new_local_engine(dir.path().to_str().unwrap(), ALL_CFS).unwrap();

}

fn bench_load() {
    let dir = TempDir::new("engine_bench").unwrap();
    let e = new_local_engine(dir.path().to_str().unwrap(), ALL_CFS).unwrap();
}

fn bench_update() {
    let dir = TempDir::new("engine_bench").unwrap();
    let e = new_local_engine(dir.path().to_str().unwrap(), ALL_CFS).unwrap();
}

fn bench_insert() {
    let dir = TempDir::new("engine_bench").unwrap();
    let e = new_local_engine(dir.path().to_str().unwrap(), ALL_CFS).unwrap();
}

fn new_write_key() -> str {}

fn new_lock_key() -> str {}

pub fn bench_engine() {
    printf!("benching tombstone scan with rocksdb\t...\t");
    //    print_result(bench_tombstone_scan());
}
