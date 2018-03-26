// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{mem, usize};
use std::time::Duration;
use std::cell::{RefCell, RefMut};
use std::sync::{mpsc, Arc};
use std::iter::FromIterator;
use std::thread::{self, ThreadId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt::{self, Debug, Display, Formatter};

use protobuf::{CodedInputStream, Message as PbMsg};
use futures::{future, stream};
use futures::sync::mpsc as futures_mpsc;
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};

use tipb::select::DAGRequest;
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::checksum::{ChecksumRequest, ChecksumScanOn};
use tipb::executor::ExecType;
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::{CommandPri, HandleTime, IsolationLevel};

use util::time::{duration_to_sec, Instant};
use util::worker::{FutureScheduler, Runnable, Scheduler};
use util::collections::HashMap;
use server::{Config, OnResponse};
use storage::{self, engine, Engine, Snapshot};
use storage::engine::Error as EngineError;
use pd::PdTask;

use super::codec::mysql;
use super::codec::datum::Datum;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::checksum::ChecksumContext;
use super::metrics::*;
use super::local_metrics::{BasicLocalMetrics, ExecLocalMetrics};
use super::dag::executor::ExecutorMetrics;
use super::{Error, Result};

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;
pub const REQ_TYPE_CHECKSUM: i64 = 105;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
pub const DEFAULT_REQUEST_MAX_HANDLE_SECS: u64 = 60;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

pub const SINGLE_GROUP: &[u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &str = "request outdated.";

const ENDPOINT_IS_BUSY: &str = "endpoint is busy";

struct CopContextInner {
    exec_local_metrics: ExecLocalMetrics,
    basic_local_metrics: BasicLocalMetrics,
    timer: Instant,
    timeout: Duration,
}

impl CopContextInner {
    fn collect(&mut self, region_id: u64, scan_tag: &str, metrics: ExecutorMetrics) {
        self.exec_local_metrics
            .collect(scan_tag, region_id, metrics);

        let new_timer = Instant::now_coarse();
        if new_timer.duration_since(self.timer) >= self.timeout {
            self.exec_local_metrics.flush();
            self.basic_local_metrics.flush();
            self.timer = new_timer;
        }
    }
}

struct CopContext(RefCell<CopContextInner>);

impl CopContext {
    fn new(pd_task_sender: FutureScheduler<PdTask>) -> Self {
        let inner = CopContextInner {
            exec_local_metrics: ExecLocalMetrics::new(pd_task_sender),
            basic_local_metrics: BasicLocalMetrics::default(),
            timer: Instant::now_coarse(),
            timeout: Duration::from_secs(1),
        };
        CopContext(RefCell::new(inner))
    }
}

#[derive(Clone)]
struct CopContextPool {
    cop_ctxs: Arc<HashMap<ThreadId, CopContext>>,
}

impl CopContextPool {
    // Must be called in thread pool.
    fn get_context(&self) -> &CopContext {
        let thread_id = thread::current().id();
        self.cop_ctxs.get(&thread_id).unwrap()
    }
}

impl FromIterator<(ThreadId, CopContext)> for CopContextPool {
    fn from_iter<T: IntoIterator<Item = (ThreadId, CopContext)>>(iter: T) -> Self {
        let mut cop_ctxs = map![];
        for (thread_id, cop_ctx) in iter {
            cop_ctxs.insert(thread_id, cop_ctx);
        }
        let cop_ctxs = Arc::new(cop_ctxs);
        CopContextPool { cop_ctxs: cop_ctxs }
    }
}

unsafe impl Sync for CopContextPool {}
unsafe impl Send for CopContextPool {}

impl Debug for CopContextPool {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "CopContextPool")
    }
}

struct ExecutorPool {
    pool: CpuPool,
    contexts: CopContextPool,
}

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ExecutorPool,
    low_priority_pool: ExecutorPool,
    high_priority_pool: ExecutorPool,
    basic_local_metrics: BasicLocalMetrics,
    max_running_task_count: usize,
    running_task_count: Arc<AtomicUsize>,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    request_max_handle_duration: Duration,
}

impl Host {
    pub fn new(
        engine: Box<Engine>,
        scheduler: Scheduler<Task>,
        cfg: &Config,
        pd_task_sender: FutureScheduler<PdTask>,
    ) -> Host {
        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            pool: Host::create_executor_pool(
                "endpoint-normal-pool",
                cfg.end_point_concurrency,
                cfg.end_point_stack_size.0 as usize,
                pd_task_sender.clone(),
            ),
            low_priority_pool: Host::create_executor_pool(
                "endpoint-low-pool",
                cfg.end_point_concurrency,
                cfg.end_point_stack_size.0 as usize,
                pd_task_sender.clone(),
            ),
            high_priority_pool: Host::create_executor_pool(
                "endpoint-high-pool",
                cfg.end_point_concurrency,
                cfg.end_point_stack_size.0 as usize,
                pd_task_sender,
            ),
            basic_local_metrics: BasicLocalMetrics::default(),
            max_running_task_count: cfg.end_point_max_tasks,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            request_max_handle_duration: cfg.end_point_request_max_handle_duration.0,
            running_task_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn create_executor_pool(
        name_prefix: &str,
        pool_size: usize,
        thread_stack_size: usize,
        pd_task_sender: FutureScheduler<PdTask>,
    ) -> ExecutorPool {
        let (tx, rx) = mpsc::sync_channel(pool_size);
        let cpu_pool = {
            CpuPoolBuilder::new()
                .name_prefix(name_prefix)
                .pool_size(pool_size)
                .stack_size(thread_stack_size)
                .after_start(move || {
                    let thread_id = thread::current().id();
                    let cop_ctx = CopContext::new(pd_task_sender.clone());
                    tx.send((thread_id, cop_ctx)).unwrap();
                })
                .create()
        };
        ExecutorPool {
            pool: cpu_pool,
            contexts: CopContextPool::from_iter(rx),
        }
    }

    #[inline]
    fn running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::Acquire)
    }

    fn notify_failed<E: Into<Error> + Debug>(&mut self, e: E, reqs: Vec<RequestTask>) {
        debug!("failed to handle batch request: {:?}", e);
        let resp = err_multi_resp(e.into(), reqs.len(), &mut self.basic_local_metrics);
        for t in reqs {
            t.on_resp.respond(resp.clone());
        }
    }

    #[inline]
    fn notify_batch_failed<E: Into<Error> + Debug>(&mut self, e: E, batch_id: u64) {
        let reqs = self.reqs.remove(&batch_id).unwrap();
        self.notify_failed(e, reqs);
    }

    fn handle_request(&mut self, snap: Box<Snapshot>, t: RequestTask) {
        let metrics = &mut self.basic_local_metrics;
        if let Err(e) = t.check_outdated() {
            let resp = err_resp(e, metrics);
            return t.on_resp.respond(resp);
        }

        let (mut req, cop_req, req_ctx, on_resp) = (t.req, t.cop_req, t.ctx, t.on_resp);
        let mut tracker = t.tracker;
        let ranges = req.take_ranges().into_vec();

        let pool_and_ctx_pool = match req.get_context().get_priority() {
            CommandPri::Low => &mut self.low_priority_pool,
            CommandPri::High => &mut self.high_priority_pool,
            CommandPri::Normal => &mut self.pool,
        };
        let pool = &pool_and_ctx_pool.pool;
        tracker.ctx_pool(pool_and_ctx_pool.contexts.clone());

        match cop_req {
            CopRequest::DAG(dag) => {
                let mut ctx = match DAGContext::new(dag, ranges, snap, req_ctx) {
                    Ok(ctx) => ctx,
                    Err(e) => return on_resp.respond(err_resp(e, metrics)),
                };
                if !on_resp.is_streaming() {
                    let batch_row_limit = self.batch_row_limit;
                    let do_request = move || {
                        tracker.record_wait();
                        let mut resp = ctx.handle_request(batch_row_limit).unwrap_or_else(|e| {
                            let mut metrics = tracker.get_basic_metrics();
                            err_resp(e, &mut metrics)
                        });
                        let mut exec_metrics = ExecutorMetrics::default();
                        ctx.collect_metrics_into(&mut exec_metrics);
                        tracker.record_handle(Some(&mut resp), exec_metrics);
                        future::ok::<_, ()>(on_resp.respond(resp))
                    };
                    return pool.spawn_fn(do_request).forget();
                }
                // For streaming.
                let batch_row_limit = self.stream_batch_row_limit;
                let s = stream::unfold((ctx, false), move |(mut ctx, finished)| {
                    if finished {
                        return None;
                    }
                    tracker.record_wait();
                    let (mut item, finished) = ctx.handle_streaming_request(batch_row_limit)
                        .unwrap_or_else(|e| {
                            let mut metrics = tracker.get_basic_metrics();
                            (Some(err_resp(e, &mut metrics)), true)
                        });
                    let mut exec_metrics = ExecutorMetrics::default();
                    ctx.collect_metrics_into(&mut exec_metrics);
                    tracker.record_handle(item.as_mut(), exec_metrics);
                    item.map(|resp| {
                        future::ok::<_, futures_mpsc::SendError<_>>((resp, (ctx, finished)))
                    })
                });
                on_resp.respond_stream(box s, pool.clone());
            }
            CopRequest::Analyze(analyze) => {
                let ctx = AnalyzeContext::new(analyze, ranges, snap, &req_ctx);
                let mut exec_metrics = ExecutorMetrics::default();
                let do_request = move || {
                    tracker.record_wait();
                    let mut resp = ctx.handle_request(&mut exec_metrics).unwrap_or_else(|e| {
                        let mut metrics = tracker.get_basic_metrics();
                        err_resp(e, &mut metrics)
                    });
                    tracker.record_handle(Some(&mut resp), exec_metrics);
                    future::ok::<_, ()>(on_resp.respond(resp))
                };
                pool.spawn_fn(do_request).forget();
            }
            CopRequest::Checksum(checksum) => {
                let ctx = ChecksumContext::new(checksum, ranges, snap, &req_ctx);
                let mut exec_metrics = ExecutorMetrics::default();
                let do_request = move || {
                    tracker.record_wait();
                    let mut resp = ctx.handle_request(&mut exec_metrics).unwrap_or_else(|e| {
                        let mut metrics = tracker.get_basic_metrics();
                        err_resp(e, &mut metrics)
                    });
                    tracker.record_handle(Some(&mut resp), exec_metrics);
                    future::ok::<_, ()>(on_resp.respond(resp))
                };
                pool.spawn_fn(do_request).forget();
            }
        }
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<Box<Snapshot>>) {
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => return self.notify_batch_failed(e, id),
        };
        for req in self.reqs.remove(&id).unwrap() {
            self.handle_request(snap.clone(), req);
        }
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
    BatchSnapRes(Vec<(u64, engine::Result<Box<Snapshot>>)>),
    RetryRequests(Vec<u64>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
            Task::BatchSnapRes(_) => write!(f, "batch snapres"),
            Task::RetryRequests(ref retry) => write!(f, "retry on task ids: {:?}", retry),
        }
    }
}

#[derive(Debug)]
enum CopRequest {
    DAG(DAGRequest),
    Analyze(AnalyzeReq),
    Checksum(ChecksumRequest),
}

#[derive(Debug)]
pub struct ReqContext {
    pub start_time: Instant,
    pub deadline: Instant,
    pub isolation_level: IsolationLevel,
    pub fill_cache: bool,
    pub table_scan: bool, // Whether is a table scan request.
}

impl ReqContext {
    #[inline]
    fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            STR_REQ_TYPE_SELECT
        } else {
            STR_REQ_TYPE_INDEX
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            let elapsed = now.duration_since(self.start_time);
            return Err(Error::Outdated(elapsed, self.get_scan_tag()));
        }
        Ok(())
    }

    pub fn set_max_handle_duration(&mut self, request_max_handle_duration: Duration) {
        self.deadline = self.start_time + request_max_handle_duration;
    }
}

#[derive(Debug)]
struct RequestTracker {
    running_task_count: Option<Arc<AtomicUsize>>,
    ctx_pool: Option<CopContextPool>,
    record_handle_time: bool,
    record_scan_detail: bool,

    exec_metrics: ExecutorMetrics,
    start: Instant, // The request start time.
    total_handle_time: f64,

    // These 4 fields are for ExecDetails.
    wait_start: Option<Instant>,
    handle_start: Option<Instant>,
    wait_time: Option<f64>,
    handle_time: Option<f64>,

    region_id: u64,
    txn_start_ts: u64,
    ranges_len: usize,
    first_range: Option<KeyRange>,
    scan_tag: &'static str,
    pri_str: &'static str,
}

impl RequestTracker {
    fn task_count(&mut self, running_task_count: Arc<AtomicUsize>) {
        running_task_count.fetch_add(1, Ordering::Release);
        self.running_task_count = Some(running_task_count);
    }

    fn ctx_pool(&mut self, ctx_pool: CopContextPool) {
        self.ctx_pool = Some(ctx_pool);
    }

    // This function will be only called in thread pool.
    fn get_basic_metrics(&self) -> RefMut<BasicLocalMetrics> {
        let ctx_pool = self.ctx_pool.as_ref().unwrap();
        let ctx = ctx_pool.get_context().0.borrow_mut();
        RefMut::map(ctx, |c| &mut c.basic_local_metrics)
    }

    // This function will be only called in thread pool.
    fn record_wait(&mut self) {
        let stop_first_wait = self.wait_time.is_none();
        let wait_start = self.wait_start.take().unwrap();
        let now = Instant::now_coarse();
        self.wait_time = Some(duration_to_sec(now - wait_start));
        self.handle_start = Some(now);

        if stop_first_wait {
            COPR_PENDING_REQS
                .with_label_values(&[self.scan_tag, self.pri_str])
                .dec();

            let ctx_pool = self.ctx_pool.as_ref().unwrap();
            let mut cop_ctx = ctx_pool.get_context().0.borrow_mut();
            cop_ctx
                .basic_local_metrics
                .wait_time
                .with_label_values(&[self.scan_tag])
                .observe(self.wait_time.unwrap());
        }
    }

    #[allow(useless_let_if_seq)]
    fn record_handle(&mut self, resp: Option<&mut Response>, mut exec_metrics: ExecutorMetrics) {
        let handle_start = self.handle_start.take().unwrap();
        let now = Instant::now_coarse();
        self.handle_time = Some(duration_to_sec(now - handle_start));
        self.wait_start = Some(now);
        self.total_handle_time += self.handle_time.unwrap();

        self.exec_metrics.merge(&mut exec_metrics);

        let mut record_handle_time = self.record_handle_time;
        let mut record_scan_detail = self.record_scan_detail;
        if self.handle_time.unwrap() > SLOW_QUERY_LOWER_BOUND {
            record_handle_time = true;
            record_scan_detail = true;
        }
        if let Some(resp) = resp {
            if record_handle_time {
                let mut handle = HandleTime::new();
                handle.set_process_ms((self.handle_time.unwrap() * 1000f64) as i64);
                handle.set_wait_ms((self.wait_time.unwrap() * 1000f64) as i64);
                resp.mut_exec_details().set_handle_time(handle);
            }
            if record_scan_detail {
                let detail = self.exec_metrics.cf_stats.scan_detail();
                resp.mut_exec_details().set_scan_detail(detail);
            }
        }
    }
}

impl Drop for RequestTracker {
    fn drop(&mut self) {
        if let Some(task_count) = self.running_task_count.take() {
            task_count.fetch_sub(1, Ordering::Release);
        }

        if self.total_handle_time > SLOW_QUERY_LOWER_BOUND {
            info!(
                "[region {}] handle {:?} [{}] takes {:?} [keys: {}, hit: {}, \
                 ranges: {} ({:?})]",
                self.region_id,
                self.txn_start_ts,
                self.scan_tag,
                self.total_handle_time,
                self.exec_metrics.cf_stats.total_op_count(),
                self.exec_metrics.cf_stats.total_processed(),
                self.ranges_len,
                self.first_range,
            );
        }
        if self.wait_time.is_none() {
            COPR_PENDING_REQS
                .with_label_values(&[self.scan_tag, self.pri_str])
                .dec();

            // For the request is failed before enter into thread pool.
            let wait_start = self.wait_start.take().unwrap();
            let now = Instant::now_coarse();
            let wait_time = duration_to_sec(now - wait_start);
            BasicLocalMetrics::default()
                .wait_time
                .with_label_values(&[self.scan_tag])
                .observe(wait_time);
            return;
        }

        let ctx_pool = self.ctx_pool.take().unwrap();
        let mut cop_ctx = ctx_pool.get_context().0.borrow_mut();

        cop_ctx
            .basic_local_metrics
            .req_time
            .with_label_values(&[self.scan_tag])
            .observe(duration_to_sec(self.start.elapsed()));
        cop_ctx
            .basic_local_metrics
            .handle_time
            .with_label_values(&[self.scan_tag])
            .observe(self.total_handle_time);
        cop_ctx
            .basic_local_metrics
            .scan_keys
            .with_label_values(&[self.scan_tag])
            .observe(self.exec_metrics.cf_stats.total_op_count() as f64);

        let exec_metrics = mem::replace(&mut self.exec_metrics, ExecutorMetrics::default());
        cop_ctx.collect(self.region_id, self.scan_tag, exec_metrics);
    }
}

#[derive(Debug)]
pub struct RequestTask {
    req: Request,
    cop_req: CopRequest,
    ctx: ReqContext,
    on_resp: OnResponse<Response>,
    tracker: RequestTracker,
}

impl RequestTask {
    pub fn new(
        req: Request,
        on_resp: OnResponse<Response>,
        recursion_limit: u32,
    ) -> Result<RequestTask> {
        let mut table_scan = false;
        let (start_ts, cop_req) = match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                }
                (dag.get_start_ts(), CopRequest::DAG(dag))
            }
            REQ_TYPE_ANALYZE => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                (analyze.get_start_ts(), CopRequest::Analyze(analyze))
            }
            REQ_TYPE_CHECKSUM => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                (checksum.get_start_ts(), CopRequest::Checksum(checksum))
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };

        let start_time = Instant::now_coarse();

        let req_ctx = ReqContext {
            start_time: start_time,
            deadline: start_time,
            isolation_level: req.get_context().get_isolation_level(),
            fill_cache: !req.get_context().get_not_fill_cache(),
            table_scan: table_scan,
        };

        let request_tracker = RequestTracker {
            running_task_count: None,
            ctx_pool: None,
            record_handle_time: req.get_context().get_handle_time(),
            record_scan_detail: req.get_context().get_scan_detail(),

            start: start_time,
            total_handle_time: 0f64,
            wait_start: Some(start_time),
            handle_start: None,
            wait_time: None,
            handle_time: None,
            exec_metrics: ExecutorMetrics::default(),

            region_id: req.get_context().get_region_id(),
            txn_start_ts: start_ts,
            ranges_len: req.get_ranges().len(),
            first_range: req.get_ranges().get(0).cloned(),
            scan_tag: req_ctx.get_scan_tag(),
            pri_str: get_req_pri_str(req.get_context().get_priority()),
        };

        COPR_PENDING_REQS
            .with_label_values(&[request_tracker.scan_tag, request_tracker.pri_str])
            .add(1.0);

        Ok(RequestTask {
            req: req,
            cop_req: cop_req,
            ctx: req_ctx,
            on_resp: on_resp,
            tracker: request_tracker,
        })
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.ctx.check_if_outdated()
    }

    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
    }

    pub fn set_max_handle_duration(&mut self, request_max_handle_duration: Duration) {
        self.ctx
            .set_max_handle_duration(request_max_handle_duration);
    }

    fn get_request_key(&self) -> (u64, u64, u64) {
        let ctx = self.req.get_context();
        let region_id = ctx.get_region_id();
        let version = ctx.get_region_epoch().get_version();
        let peer_id = ctx.get_peer().get_id();
        (region_id, version, peer_id)
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "request [context {:?}, tp: {}, ranges: {} ({:?})]",
            self.req.get_context(),
            self.req.get_tp(),
            self.req.get_ranges().len(),
            self.req.get_ranges().get(0)
        )
    }
}

impl Runnable<Task> for Host {
    // TODO: limit pending reqs
    fn run(&mut self, _: Task) {
        panic!("Shouldn't call Host::run directly");
    }

    #[allow(for_kv_map)]
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(mut req) => {
                    req.set_max_handle_duration(self.request_max_handle_duration);
                    if let Err(e) = req.check_outdated() {
                        let resp = err_resp(e, &mut self.basic_local_metrics);
                        req.on_resp.respond(resp);
                        continue;
                    }
                    let key = req.get_request_key();
                    grouped_reqs.entry(key).or_insert_with(Vec::new).push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    self.handle_snapshot_result(q_id, snap_res);
                }
                Task::BatchSnapRes(batch) => for (q_id, snap_res) in batch {
                    self.handle_snapshot_result(q_id, snap_res);
                },
                Task::RetryRequests(retry) => for id in retry {
                    if let Err(e) = {
                        let ctx = self.reqs[&id][0].req.get_context();
                        let sched = self.sched.clone();
                        self.engine.async_snapshot(ctx, box move |(_, res)| {
                            sched.schedule(Task::SnapRes(id, res)).unwrap()
                        })
                    } {
                        self.notify_batch_failed(e, id);
                    }
                },
            }
        }

        if grouped_reqs.is_empty() {
            return;
        }

        let mut batch = Vec::with_capacity(grouped_reqs.len());
        let start_id = self.last_req_id + 1;
        for (_, mut reqs) in grouped_reqs {
            let max_running_task_count = self.max_running_task_count;
            if self.running_task_count() >= max_running_task_count {
                self.notify_failed(Error::Full(max_running_task_count), reqs);
                continue;
            }

            for req in &mut reqs {
                let task_count = Arc::clone(&self.running_task_count);
                req.tracker.task_count(task_count);
            }
            self.last_req_id += 1;
            batch.push(reqs[0].req.get_context().clone());
            self.reqs.insert(self.last_req_id, reqs);
        }
        let end_id = self.last_req_id;

        let sched = self.sched.clone();
        let on_finished: engine::BatchCallback<Box<Snapshot>> = box move |results: Vec<_>| {
            let mut ready = Vec::with_capacity(results.len());
            let mut retry = Vec::new();
            for (id, res) in (start_id..end_id + 1).zip(results) {
                match res {
                    Some((_, res)) => ready.push((id, res)),
                    None => retry.push(id),
                }
            }

            if !ready.is_empty() {
                sched.schedule(Task::BatchSnapRes(ready)).unwrap();
            }
            if !retry.is_empty() {
                BATCH_REQUEST_TASKS
                    .with_label_values(&["retry"])
                    .observe(retry.len() as f64);
                sched.schedule(Task::RetryRequests(retry)).unwrap();
            }
        };

        BATCH_REQUEST_TASKS
            .with_label_values(&["all"])
            .observe(batch.len() as f64);

        if let Err(e) = self.engine.async_batch_snapshot(batch, on_finished) {
            for id in start_id..end_id + 1 {
                let err = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot batch failed error {:?}", e);
                    EngineError::Other(box_err!("{:?}", e))
                });
                self.notify_batch_failed(err, id);
            }
        }

        self.basic_local_metrics.flush();
    }
}

fn err_multi_resp(e: Error, count: usize, metrics: &mut BasicLocalMetrics) -> Response {
    let mut resp = Response::new();
    let count = count as f64;
    let tag = match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
            tag
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            "lock"
        }
        Error::Outdated(elapsed, scan_tag) => {
            metrics
                .outdate_time
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);
            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
            "outdated"
        }
        Error::Full(allow) => {
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit {}", allow));
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
            "full"
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            "other"
        }
    };
    metrics
        .error_cnt
        .with_label_values(&[tag])
        .inc_by(count)
        .unwrap();
    resp
}

pub fn err_resp(e: Error, metrics: &mut BasicLocalMetrics) -> Response {
    err_multi_resp(e, 1, metrics)
}

pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

pub const STR_REQ_TYPE_SELECT: &str = "select";
pub const STR_REQ_TYPE_INDEX: &str = "index";
pub const STR_REQ_PRI_LOW: &str = "low";
pub const STR_REQ_PRI_NORMAL: &str = "normal";
pub const STR_REQ_PRI_HIGH: &str = "high";

#[inline]
pub fn get_req_pri_str(pri: CommandPri) -> &'static str {
    match pri {
        CommandPri::Low => STR_REQ_PRI_LOW,
        CommandPri::Normal => STR_REQ_PRI_NORMAL,
        CommandPri::High => STR_REQ_PRI_HIGH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc;

    use kvproto::coprocessor::Request;
    use tipb::select::DAGRequest;
    use tipb::expression::Expr;
    use tipb::executor::Executor;

    use util::config::ReadableDuration;
    use util::time::Instant;
    use util::worker::{Builder as WorkerBuilder, FutureWorker};

    #[test]
    fn test_get_reg_scan_tag() {
        let mut ctx = ReqContext {
            start_time: Instant::now_coarse(),
            deadline: Instant::now_coarse(),
            isolation_level: IsolationLevel::RC,
            fill_cache: true,
            table_scan: true,
        };
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_SELECT);
        ctx.table_scan = false;
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_INDEX);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(30).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_request_max_handle_duration = ReadableDuration::secs(0);
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        worker.start(end_point).unwrap();

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        let (tx, rx) = mpsc::channel();
        let on_resp = OnResponse::Unary(box move |msg| tx.send(msg).unwrap());
        let task = RequestTask::new(req, on_resp, 1000).unwrap();

        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
        worker.stop();
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(5).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let mut end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        end_point.max_running_task_count = 1;
        worker.start(end_point).unwrap();
        let (tx, rx) = mpsc::channel();
        for pos in 0..30 * 4 {
            let tx = tx.clone();
            let mut req = Request::new();
            req.set_tp(REQ_TYPE_DAG);
            if pos % 3 == 0 {
                req.mut_context().set_priority(CommandPri::Low);
            } else if pos % 3 == 1 {
                req.mut_context().set_priority(CommandPri::Normal);
            } else {
                req.mut_context().set_priority(CommandPri::High);
            }
            let on_resp = OnResponse::Unary(box move |msg| {
                thread::sleep(Duration::from_millis(100));
                let _ = tx.send(msg); // To avoid panic if rx is closed.
            });

            let task = RequestTask::new(req, on_resp, 1000).unwrap();
            worker.schedule(Task::Request(task)).unwrap();
        }
        for _ in 0..120 {
            let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if !resp.has_region_error() {
                continue;
            }
            assert!(resp.get_region_error().has_server_is_busy());
            return;
        }
        panic!("suppose to get ServerIsBusy error.");
    }

    #[test]
    fn test_stack_guard() {
        let mut expr = Expr::new();
        for _ in 0..10 {
            let mut e = Expr::new();
            e.mut_children().push(expr);
            expr = e;
        }
        let mut e = Executor::new();
        e.mut_selection().mut_conditions().push(expr);
        let mut dag = DAGRequest::new();
        dag.mut_executors().push(e);
        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());

        let err = RequestTask::new(req, OnResponse::Unary(box |_| ()), 5).unwrap_err();
        let s = format!("{:?}", err);
        assert!(
            s.contains("Recursion"),
            "parse should fail due to recursion limit {}",
            s
        );
    }
}
