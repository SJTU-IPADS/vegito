#pragma once

#include <memory>
#include "core/graph.hpp"
#include "core/transaction.hpp"
#include "core/segment_graph.hpp"
#include "core/segment_transaction.hpp"
#include "core/csr_graph.hpp"
#include "core/csr_transaction.hpp"
#include "core/epoch_graph_reader.hpp"
#include "core/epoch_graph_writer.hpp"

namespace nocc {
namespace oltp {
namespace wiki {

class WikiLoader {
public:
    WikiLoader(std::string dataset_dir, int file_num);
    void load_wiki_seg_graph(std::shared_ptr<livegraph::SegGraph> seg_graph);
    void load_wiki_live_graph(std::shared_ptr<livegraph::Graph> graph);
    void load_wiki_seg_graph_random(std::shared_ptr<livegraph::SegGraph> seg_graph);
    void load_wiki_live_graph_random(std::shared_ptr<livegraph::Graph> graph);
    void load_wiki_csr_graph(std::shared_ptr<livegraph::CSRGraph> csr_graph, bool random = false);

protected:
    void load_triples();
    
    int file_num_;
    std::string dataset_dir_;
};

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
