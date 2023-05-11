#pragma once

#include <memory>

#include "core/segment_graph.hpp"
#include "utils/thread.h"

namespace nocc {
namespace oltp {
namespace wiki {

class EpochTimer : public ndb_thread {
public:
    EpochTimer(std::shared_ptr<livegraph::SegGraph> graph, double epoch_sec);

    void run() override;

protected:
    const uint64_t SECOND_CYCLE_;
    const double epoch_sec_;
    std::shared_ptr<livegraph::SegGraph> graph_;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
