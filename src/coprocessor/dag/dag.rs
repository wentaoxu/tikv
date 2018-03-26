// Copyright 2017 PingCAP, Inc.
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

use std::sync::Arc;

use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, EncodeType, SelectResponse, StreamResponse};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::dag::expr::EvalConfig;
use coprocessor::Result;
use coprocessor::endpoint::{get_pk, ReqContext};
use storage::{Snapshot, SnapshotStore};

use super::executor::{build_exec, Executor, ExecutorMetrics, Row};

pub struct DAGContext {
    columns: Arc<Vec<ColumnInfo>>,
    has_aggr: bool,
    req_ctx: ReqContext,
    exec: Box<Executor + Send>,
    output_offsets: Vec<u32>,
}

impl DAGContext {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: ReqContext,
    ) -> Result<DAGContext> {
        let eval_cfg = Arc::new(box_try!(EvalConfig::new(
            req.get_time_zone_offset(),
            req.get_flags()
        )));
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );

        let dag_executor = build_exec(req.take_executors().into_vec(), store, ranges, eval_cfg)?;
        Ok(DAGContext {
            columns: dag_executor.columns,
            has_aggr: dag_executor.has_aggr,
            req_ctx: req_ctx,
            exec: dag_executor.exec,
            output_offsets: req.take_output_offsets(),
        })
    }

    pub fn handle_request(&mut self, batch_row_limit: usize) -> Result<Response> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        loop {
            match self.exec.next()? {
                Some(row) => {
                    self.req_ctx.check_if_outdated()?;
                    if chunks.is_empty() || record_cnt >= batch_row_limit {
                        let chunk = Chunk::new();
                        chunks.push(chunk);
                        record_cnt = 0;
                    }
                    let chunk = chunks.last_mut().unwrap();
                    record_cnt += 1;
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                }
                None => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_chunks(RepeatedField::from_vec(chunks));
                    self.exec
                        .collect_output_counts(sel_resp.mut_output_counts());
                    let data = box_try!(sel_resp.write_to_bytes());
                    resp.set_data(data);
                    return Ok(resp);
                }
            }
        }
    }

    pub fn handle_streaming_request(
        &mut self,
        batch_row_limit: usize,
    ) -> Result<(Option<Response>, bool)> {
        let (mut record_cnt, mut finished) = (0, false);
        let mut chunk = Chunk::new();
        self.exec.start_scan();
        while record_cnt < batch_row_limit {
            match self.exec.next()? {
                Some(row) => {
                    record_cnt += 1;
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }
        if record_cnt > 0 {
            let range = self.exec.stop_scan();
            return self.make_stream_response(chunk, range)
                .map(|r| (Some(r), finished));
        }
        Ok((None, true))
    }

    fn make_stream_response(&mut self, chunk: Chunk, range: Option<KeyRange>) -> Result<Response> {
        let mut s_resp = StreamResponse::new();
        s_resp.set_encode_type(EncodeType::TypeDefault);
        s_resp.set_data(box_try!(chunk.write_to_bytes()));
        self.exec.collect_output_counts(s_resp.mut_output_counts());

        let mut resp = Response::new();
        resp.set_data(box_try!(s_resp.write_to_bytes()));
        if let Some(range) = range {
            resp.set_range(range);
        }
        Ok(resp)
    }

    pub fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.exec.collect_metrics_into(metrics);
    }
}

#[inline]
fn inflate_cols(row: &Row, cols: &[ColumnInfo], output_offsets: &[u32]) -> Result<Vec<u8>> {
    let data = &row.data;
    // TODO capacity is not enough
    let mut values = Vec::with_capacity(data.value.len());
    for offset in output_offsets {
        let col = &cols[*offset as usize];
        let col_id = col.get_column_id();
        match data.get(col_id) {
            Some(value) => values.extend_from_slice(value),
            None if col.get_pk_handle() => {
                let pk = get_pk(col, row.handle);
                box_try!(values.encode(&[pk], false));
            }
            None if col.has_default_val() => {
                values.extend_from_slice(col.get_default_val());
            }
            None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                return Err(box_err!("column {} of {} is missing", col_id, row.handle));
            }
            None => {
                box_try!(values.encode(&[Datum::Null], false));
            }
        }
    }
    Ok(values)
}
