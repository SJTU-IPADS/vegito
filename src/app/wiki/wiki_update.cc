#include <fstream>
#include "wiki_update.h"

using namespace livegraph;

namespace nocc {
namespace oltp {
namespace wiki {

WikiUpdateWorker::WikiUpdateWorker(uint32_t worker_id, uint32_t num_thread, 
                                 std::shared_ptr<SegGraph> graph, std::string dataset_dir) 
    : worker_id_(worker_id), num_thread_(num_thread), graph_(graph), dataset_dir_(dataset_dir) {
    
}

void WikiUpdateWorker::run() {
    /* Bind core */
    // int cpu_num = BindToCore(worker_id_);
    wiki_update_seg_graph();
}

void WikiUpdateWorker::wiki_update_seg_graph() {
    nocc::util::Breakdown_Timer timer;
    timer.start();

    auto txn = graph_->begin_batch_loader();
    std::vector<std::pair<vertex_t, vertex_t>> edges;
    auto load_triples = [&](std::string fname) {
        std::cout << "[Update Worker " << worker_id_ << "] " << "load edges from " << fname << std::endl;
        std::ifstream ifile(fname.c_str());

        vertex_t s;
        label_t p = 0;
        vertex_t o;
        while (ifile >> s >> o) {
            edges.emplace_back(s, o);
        }
    };

    for(int i = 0; i <= 12; i++) {
        if(i % num_thread_ == worker_id_)
            load_triples(dataset_dir_ + "id_wiki_" + std::to_string(i));
    }
    // std::cout << "[Update Worker " << worker_id_ << "] " << "Start shuffling..." << std::endl;
    // std::random_shuffle(edges.begin(),edges.end());

    std::cout << "[Update Worker " << worker_id_ << "] " << "Start putting edges..." << std::endl;
    for (const auto &p : edges)
    {
        txn.put_edge(p.first, 0, p.second, "");
    }

    std::cout << "[Update Worker " << worker_id_ << "] " << "finish loading edges, "
              << " time:" << timer.get_diff_ms()/1000 << " seconds" << std::endl;
}

void WikiUpdateWorker::update() {
    std::string dir = "/home/sl/id_snb_0.1/";
    auto load_triples = [&](std::string fname) {
        std::ifstream ifile(fname.c_str());

        vertex_t s;
        label_t p;
        vertex_t o;
        while (ifile >> s >> p >> o) {
            auto txn = graph_->begin_transaction();
            txn.put_edge(s, p, o, "");
            txn.commit();
        }
    };

    load_triples(dir + "update_stream.ttl");
    std::cout << "Finish updating the graph!" << std::endl;
}

}  // namespace wiki
}  // namespace oltp
}  // namespace nocc
