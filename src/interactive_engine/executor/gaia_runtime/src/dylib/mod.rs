//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#![allow(bare_trait_objects)]

use crate::server::init_with_rpc_service;
use crate::server::manager::GaiaServerManager;

use gaia_pegasus::api::{Count, Fold, FoldByKey, KeyBy, Map, Sink, Source};
use gaia_pegasus::result::{ResultSink, ResultStream};
use gaia_pegasus::stream::Stream;
use gaia_pegasus::{run_opt, BuildJobError, Configuration, JobConf, StartupError};

use gremlin_core::compiler::GremlinJobCompiler;
use gremlin_core::process::traversal::path::ResultPath;
use gremlin_core::process::traversal::step::accum::Accumulator;
use gremlin_core::process::traversal::step::functions::EncodeFunction;
use gremlin_core::process::traversal::step::result_downcast::{
    try_downcast_list, try_downcast_pair,
};
use gremlin_core::process::traversal::step::{graph_step_from, ResultProperty};
use gremlin_core::process::traversal::traverser::{Requirement, Traverser};
use gremlin_core::structure::{Details, PropKey, Tag, VertexOrEdge};
use gremlin_core::{create_demo_graph, str_to_dyn_error, DynIter, Element, Partitioner, ID};
use gremlin_core::{GremlinStepPb, Partition};
use grpcio::ChannelBuilder;
use grpcio::EnvBuilder;
use gs_gremlin::{InitializeJobCompiler, QueryVineyard, create_gs_store};
use maxgraph_common::proto::data::*;
use maxgraph_common::proto::hb::*;
use maxgraph_common::proto::query_flow::*;
use maxgraph_common::util;
use maxgraph_common::util::get_local_ip;
use maxgraph_common::util::log4rs::init_log4rs;
use maxgraph_runtime::server::manager::*;
use maxgraph_runtime::server::RuntimeInfo;
use maxgraph_server::StoreContext;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::prelude::*;
use maxgraph_store::config::{StoreConfig, VINEYARD_GRAPH};
use pegasus_server::pb as server_pb;
use pegasus_server::pb::AccumKind;
use pegasus_server::rpc::{start_rpc_server, RpcService};
use pegasus_server::service::{JobParser, Service};
use pegasus_server::{JobRequest, JobResponse};
use prost::Message;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use tokio::runtime::Runtime;

// fn main() {
//     if let Some(_) = env::args().find(|arg| arg == "--show-build-info") {
//         util::get_build_info();
//         return;
//     }
//     // init_log4rs();

//     initialize_pegasus();
//     evaluate_query_plan();
// }

#[no_mangle]
pub unsafe extern "C" fn initialize_pegasus(worker_num: i32) {
    let mut config: HashMap<String, String> = HashMap::new();

    config.insert("node.idx".to_string(), "0".to_string());
    config.insert("graph.name".to_string(), "g".to_string());
    config.insert("partition.count".to_string(), "1".to_string());
    config.insert("graph.port".to_string(), "0".to_string());
    config.insert("query.port".to_string(), "0".to_string());
    config.insert("engine.port".to_string(), "0".to_string());
    config.insert("worker.per.process".to_string(), worker_num.to_string());
    config.insert("worker.num".to_string(), "1".to_string());

    let mut store_config = StoreConfig::init_from_config(&config);
    info!("{:?}", store_config);
    let store_config = Arc::new(store_config);
    if store_config.graph_type.to_lowercase().eq(VINEYARD_GRAPH) {
        info!(
            "Start executor with vineyard graph object id {:?}",
            store_config.vineyard_graph_id
        );

        pegasus_common::logs::init_log();
        match gaia_pegasus::startup(Configuration::singleton()) {
            Ok(_) => {
                println!("gaia (pegasus) engine initialized");
            }
            Err(err) => match err {
                StartupError::AlreadyStarted(_) => {}
                _ => panic!("start pegasus failed"),
            }
        }
    } else {
        unimplemented!("only start vineyard graph from executor")
    }
}

#[no_mangle]
pub unsafe extern "C" fn initialize_graph(ctx: i64, worker_num: i32) {
    use maxgraph_runtime::store::ffi::FFIGraphStore;
    let ffi_store = FFIGraphStore::new(ctx, worker_num);
    let partition_manager = ffi_store.get_partition_manager();
    create_gs_store(Arc::new(ffi_store), Arc::new(partition_manager));
}

#[no_mangle]
pub unsafe extern "C" fn evaluate_query_plan(query_name: *const c_char, worker_num: i32) {
    let query_plan_name = unsafe { CStr::from_ptr(query_name).to_string_lossy().into_owned() };
    let query_plan = match env::var("QUERY_PLAN_ROOT") {
        Ok(path) => path + "/" + &query_plan_name,
        Err(_)   => query_plan_name,
    };
    read_and_evaluate_query_plan(query_plan, worker_num);
}

#[no_mangle]
pub unsafe extern "C" fn evaluate_query_plan_ith(query_index: i32, worker_num: i32) {
    let queries = vec![
        "ldbc_query_1.plan",
        "ldbc_query_2.plan",
        "ldbc_query_3.plan",
        "ldbc_query_4.plan",
        "ldbc_query_5.plan",
        "ldbc_query_6.plan",
        "ldbc_query_7.plan",
        "ldbc_query_8.plan",
        "ldbc_query_9.plan",
        "ldbc_query_10.plan",
        "ldbc_query_11.plan",
        "ldbc_query_12.plan",
    ];

    let query_plan_root = env::var("QUERY_PLAN_ROOT").unwrap();
    let query_plan = query_plan_root + "/" + queries[query_index as usize];
    read_and_evaluate_query_plan(query_plan, worker_num);
}

pub fn read_and_evaluate_query_plan(query_plan: String, worker_num: i32) {
    // run request
    let pb_request = read_pb_request(query_plan.clone())
        .expect("read pb failed");
    println!("executing query plan {:?}: {:?}", query_plan, pb_request);
    let test_job_factory = &TestJobFactory::new();
    submit_query(test_job_factory, pb_request, worker_num as u32);
}

pub fn read_pb_request<P: AsRef<Path>>(file_name: P) -> Option<JobRequest> {
    if let Ok(content) = std::fs::read(&file_name) {
        {
            if let Ok(pb_request) = JobRequest::decode(&content[0..]) {
                Some(pb_request)
            } else {
                println!("downcast pb_request failed");
                None
            }
        }
    } else {
        println!("read file {:?} failed", file_name.as_ref());
        None
    }
}

pub struct TestJobFactory {
    inner: GremlinJobCompiler,
    requirement: Requirement,
    is_ordered: bool,
}

impl TestJobFactory {
    pub fn new() -> Self {
        TestJobFactory {
            inner: GremlinJobCompiler::new(Partition { num_servers: 1 }, 1, 0),
            requirement: Requirement::OBJECT,
            is_ordered: false,
        }
    }
}

impl TestJobFactory {
    fn gen_source(
        &self, src: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Traverser> + Send>, BuildJobError> {
        let mut step = GremlinStepPb::decode(&src[0..])
            .map_err(|e| format!("protobuf decode failure: {}", e))?;
        println!("source pb step {:?}", step);
	let worker_id = gaia_pegasus::get_current_worker();
        let job_workers = worker_id.local_peers as usize;
        let mut step = graph_step_from(
            &mut step,
            job_workers,
            worker_id.index,
            self.inner.get_partitioner(),
        )?;
        // TODO: here is the bug
	//  step.set_requirement(self.requirement);
        Ok(step.gen_source(worker_id.index as usize))
    }
}

impl JobParser<Traverser, Traverser> for TestJobFactory {
    fn parse(
        &self, plan: &JobRequest, input: &mut Source<Traverser>, output: ResultSink<Traverser>,
    ) -> Result<(), BuildJobError> {
        if let Some(source) = plan.source.as_ref() {
            let source = input.input_from(self.gen_source(source.resource.as_ref())?)?;
            let stream = if let Some(task) = plan.plan.as_ref() {
                self.inner.install(source, &task.plan)?
            } else {
                source
            };
            match plan.sink.as_ref().unwrap().sinker.as_ref() {
                // TODO: more sink process here
                Some(server_pb::sink::Sinker::Fold(fold)) => {
                    let accum_kind: server_pb::AccumKind =
                        unsafe { std::mem::transmute(fold.accum) };
                    match accum_kind {
                        AccumKind::Cnt => stream
                            .count()?
                            .into_stream()?
                            .map(|v| Ok(Traverser::Object(v.into())))?
                            .sink_into(output),
                        _ => todo!(),
                    }
                }
                _ => stream.sink_into(output),
            }
        } else {
            Err("source of job not found".into())
        }
    }
}

pub fn submit_query(factory: &TestJobFactory, job_req: JobRequest, num_workers: u32) {
    let job_config = job_req.conf.clone().expect("no job_conf");
    let conf = JobConf::with_id(job_config.job_id, job_config.job_name, num_workers);
    let (tx, rx) = crossbeam_channel::unbounded();
    let sink = ResultSink::new(tx);
    let cancel_hook = sink.get_cancel_hook().clone();
    let mut results = ResultStream::new(conf.job_id, cancel_hook, rx);
    run_opt(conf, sink, |worker| {
        worker.dataflow(|input, output| factory.parse(&job_req, input, output))
    })
    .expect("submit job failure;");

    let mut trav_results = vec![];
    while let Some(result) = results.next() {
        match result {
            Ok(res) => {
                trav_results.push(res);
            }
            Err(e) => {
                panic!("err result {:?}", e);
            }
        }
    }
    dbg!("query finished");
    dbg!(trav_results);
}
