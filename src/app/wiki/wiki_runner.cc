// #include "bench_listener.h"
#include "bench_listener.h"
#include "app/analytics/grape/grape_app.h"
#include "wiki_runner.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

void WikiRunner::init_graph() {
    WikiLoader loader(config_.getDatasetDir(), config_.getFileNum());

    if (config_.getGraphType() == "seggraph") {
      if (config_.getRandom()) loader.load_wiki_seg_graph_random(this->seg_graph_);
      else loader.load_wiki_seg_graph(this->seg_graph_);
      std::cout << "Memory usage: " << this->seg_graph_->get_block_manager().getUsedMemory() / 1024.0 / 1024 << "MB" << std::endl;      
    } else if (config_.getGraphType() == "livegraph") {
      if (config_.getRandom()) loader.load_wiki_live_graph_random(this->live_graph_);
      else loader.load_wiki_live_graph(this->live_graph_);
      std::cout << "Memory usage: " << this->live_graph_->get_block_manager().getUsedMemory() / 1024.0 / 1024 << "MB" << std::endl;
    } else {
      loader.load_wiki_csr_graph(this->csr_graph_, config_.getRandom());
    }
}

void WikiRunner::run() {
    /* Init segment graph store */
    init_graph();
    std::cout << "Load finished!" << std::endl;
    /* Run client */
    // TODO:

    /* Run epoch timer */
    epoch_timer_ = std::make_shared<EpochTimer>(seg_graph_, config_.getEpochSec());
    epoch_timer_->start();

    /* Run query worker */
    query_workers_ = make_query_workers();

    for (auto worker : query_workers_)
        worker->start();

    /*Run update worker */
    update_workers_ = make_update_workers();

    for (auto worker : update_workers_)
        worker->start();

    /* Run analytics worker */
    bool use_grape_engine = false;
    if(use_grape_engine) {
#ifdef WITH_GRAPE
      google::InitGoogleLogging("analytical_apps");
      //grape::Init();
      // grape::Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType>(live_graph_, config_);
      //grape::Finalize();
#endif
    } else {
      analytics_workers_ = make_analytics_workers();

      for (auto worker : analytics_workers_)
          worker->start();
    }

    scan_workers_ = make_scan_workers();

    for (auto worker : scan_workers_)
        worker->start();

    /* Report benchmark result */
    // Reporter reporter();
    // reporter.start();

    // TODO: use this thread as listener directly
    Listener(config_, update_workers_, query_workers_, analytics_workers_, scan_workers_).run();
}

std::vector<std::shared_ptr<WikiUpdateWorker>> 
WikiRunner::make_update_workers() {
  std::vector<std::shared_ptr<WikiUpdateWorker>> ret;

  int num_update_threads = config_.getNumUpdateThreads();

  for (int worker_idx = 0; worker_idx < num_update_threads; worker_idx++) {
    ret.push_back(std::make_shared<WikiUpdateWorker>(worker_idx, num_update_threads, this->seg_graph_, config_.getDatasetDir()));
  }

  return ret;
}

std::vector<std::shared_ptr<WikiQueryWorker>> 
WikiRunner::make_query_workers() {
  std::vector<std::shared_ptr<WikiQueryWorker>> ret;

  int num_query_threads = config_.getNumQueryThreads();

  for (int worker_idx = 0; worker_idx < num_query_threads; worker_idx++) {
    ret.push_back(std::make_shared<WikiQueryWorker>(worker_idx, num_query_threads, this->seg_graph_));
  }

  return ret;
}

std::vector<std::shared_ptr<WikiAnalyticsWorker>> 
WikiRunner::make_analytics_workers() {
  std::vector<std::shared_ptr<WikiAnalyticsWorker>> ret;
  int num_analytics_threads = config_.getNumAnalyticsThreads();

  if (num_analytics_threads > 0) {
    std::string graph_type = config_.getGraphType();
    auto worker_barrier = std::make_shared<Barrier>(num_analytics_threads);

    for (int worker_idx = 0; worker_idx < num_analytics_threads; worker_idx++) {
      ret.push_back(std::make_shared<WikiAnalyticsWorker>(worker_idx, 
                    num_analytics_threads,
                    graph_type,
                    this->seg_graph_, 
                    this->live_graph_, 
                    this->csr_graph_,
                    worker_barrier));
    }

    ret[0]->set_sub_workers(ret);
  }

  return ret;
}

std::vector<std::shared_ptr<WikiScanWorker>>
WikiRunner::make_scan_workers() {
  std::vector<std::shared_ptr<WikiScanWorker>> ret;
  int num_scan_threads = config_.getNumScanThreads();
  std::string graph_type = config_.getGraphType();

  for (int idx = 0; idx < num_scan_threads; idx++) {
    ret.push_back(std::make_shared<WikiScanWorker>(graph_type, this->seg_graph_, 
                    this->live_graph_, this->csr_graph_));
  }

  return ret;
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc

