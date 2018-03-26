use mio::{self, EventLoop, EventLoopConfig, Sender};
use pd::{PdClient, PdRunner, PdTask};
pub use self::types::{Key, KvPair, make_key, MvccInfo, Value};
use std::{str, u64};
use std::sync::{Arc, RwLock};
use storage::engine::Engine;
use storage::{Key, KvPair, ScanMode, Snapshot, Statistics, Value};
use raftstore::store::{keys, Engines};

use super::Result;
use kvproto::kvrpcpb::IsolationLevel::SI;

const GC_SAFEPOINT: &str = "transaction/gc/safepoint";

pub struct gc_worker<C: 'static> {
    engine: Box<Engine>,
    pd_client: Arc<C>,

    safepoint: u64,

    refresh_safepoint_tick_interval: u64,
    gc_one_region_tick_interval: u64,
}

pub enum Tick {
    refresh_safepoint,
    gc_one_region {
        scan_key: Vec<u8>,
    },
}

impl fmt::Debug for Tick {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Tick::refresh_safepoint => write!(fmt, "refresh safepoint"),
            Tick::gc_one_region{scan_key } => write!(fmt, "gc one region, last key is {:?}", scan_key),
        }
    }
}

impl <C: PdClient> gc_worker<C> {
    pub fn new(engine: Box<Engine>, pd_client: Arc<C>, ) -> gc_worker<C> {
        gc_worker {
            engine,
            pd_client,
            safepoint: 0,
            refresh_safepoint_tick_interval: 600000,
            gc_one_region_tick_interval: 1000,
        }
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        self.register_refresh_safepoint_tick(event_loop);
        self.register_gc_one_region_tick(event_loop, keys::DATA_MIN_KEY.to_vec());

        event_loop.run(self)?;
        Ok(())
    }

    fn stop(&mut self, event_loop: &mut EventLoop<Self>) {
        event_loop.shutdown();
    }

    fn register_refresh_safepoint_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = self.register_timer(
            event_loop,
            Tick::refresh_safepoint,
            self.refresh_safepoint_tick_interval.as_millis(),
        ) {
            error!("{} register raft base tick err: {:?}", self.tag, e);
        };
    }

    fn register_gc_one_region_tick(&mut self, event_loop: &mut EventLoop<Self>, scan_key: Vec<u8>) {
        if let Err(e) = self.register_time(
            event_loop,
            Tick::gc_one_region { scan_key },
            self.gc_one_region_tick_interval.as_millis(),
        ) {
            error!("{} register raft base tick err: {:?}", self.tag, e);
        };
    }

    fn on_refresh_safepoint_tick(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        let (_, value) = self.pd_client.get_user_kv(GC_SAFEPOINT)?;
        let safepoint = box_try!(u64::from_str_radix(value, 10));
        if safepoint != 0 {
            self.safepoint = safepoint;
        }

        self.register_refresh_safepoint_tick(event_loop);
        Ok(())
    }

    fn get_next_region() -> Result<u64> {

    }

    fn on_gc_one_region_tick(&mut self, event_loop: &mut EventLoop<Self>, scan_key: Vec<u8>) -> Result<()> {
        // find the region
        let ctx = Context::new();
        let snapshot = box_try!(self.engine.snapshot(&ctx));

        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            SI,
        );

        // scan_key is used as start_key here,and Range start gc with scan_key=none.
        let is_range_start_gc = scan_key.is_none();
        // This is an optimization to skip gc before scanning all data.
        let need_gc = if is_range_start_gc {
            reader.need_gc(safe_point, ratio_threshold)
        } else {
            true
        };


        if
        // txn gc
        // engine write

        // next key and next region
        let next_scan_ley = scan_key;
        self.register_gc_one_region_tick(event_loop, next_scan_key);

        Ok(())
    }

    fn register_time(&self,
        event_loop: &mut EventLoop<gc_worker<C>>,
        tick: Tick,
        delay: u64,
    ) -> Result<()> {
        // TODO: now mio TimerError doesn't implement Error trait,
        // so we can't use `try!` directly.
        if !event_loop.is_running() || delay == 0 {
            // 0 delay means turn off the timer.
            return Ok(());
        }
        if let Err(e) = event_loop.timeout_ms(tick, delay) {
            return Err(box_err!(
            "failed to register timeout [{:?}, delay: {:?}ms]: {:?}",
            tick,
            delay,
            e
        ));
        }
        Ok(())
    }
}

impl <C: PdClient> mio::Handler for gc_worker<C> {
    type Timeout = Tick;
    type Message = Msg;

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        let t = SlowTimer::new();
        match timeout {
            Tick::refresh_safepoint => {self.on_refresh_safepoint_tick(event_loop);}
            Tick::gc_one_region { scan_key } => {self.on_gc_one_region_tick(event_loop, scan_key);}
        }
        slow_log!(t, "{} handle timeout {:?}", self.tag, timeout);
    }
}