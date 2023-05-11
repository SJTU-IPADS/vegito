#pragma once

#include <memory>

#include "epoch_timer.h"
#include "wiki_config.h"
#include "wiki_loader.h"
#include "wiki_update.h"
#include "wiki_query.h"
#include "wiki_analytics.h"
#include "wiki_scan.h"

namespace nocc {
namespace oltp {
namespace wiki {

class WikiRunner {
public:
    WikiRunner(WikiConfig config) : config_(config) {
        this->live_graph_ = std::make_shared<livegraph::Graph>();
        this->seg_graph_ = std::make_shared<livegraph::SegGraph>();
        this->csr_graph_ = std::make_shared<livegraph::CSRGraph>();
    };

    ~WikiRunner() {}

    void run();

    std::vector<std::shared_ptr<WikiUpdateWorker>> make_update_workers();
    std::vector<std::shared_ptr<WikiQueryWorker>> make_query_workers();
    std::vector<std::shared_ptr<WikiAnalyticsWorker>> make_analytics_workers();
    std::vector<std::shared_ptr<WikiScanWorker>> make_scan_workers();
    void init_graph();

protected:
    WikiConfig config_;
    std::shared_ptr<livegraph::Graph> live_graph_;
    std::shared_ptr<livegraph::SegGraph> seg_graph_;
    std::shared_ptr<livegraph::CSRGraph> csr_graph_;
    std::shared_ptr<EpochTimer> epoch_timer_;
    std::vector<std::shared_ptr<WikiUpdateWorker>> update_workers_;
    std::vector<std::shared_ptr<WikiQueryWorker>> query_workers_;
    std::vector<std::shared_ptr<WikiAnalyticsWorker>> analytics_workers_;
    std::vector<std::shared_ptr<WikiScanWorker>> scan_workers_;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
