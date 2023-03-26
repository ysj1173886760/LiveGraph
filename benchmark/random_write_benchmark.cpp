/**
 * @file basic_benchmark.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-01-27
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <exception>
#include <limits>
#include <ostream>
#include <random>
#include <gflags/gflags.h>
#include "core/graph.hpp"
#include "core/livegraph.hpp"
#include "core/transaction.hpp"
#include <iostream>

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

static const std::string db_name = "random_write_benchmark";
const int64_t point_per_thread = 1000;
const int64_t edge_per_point = 1000;
const int thread_num = 8;
const int point_num = point_per_thread * thread_num;
bool wait_visible = true;
std::atomic_int64_t qps[thread_num];

void Work(livegraph::Graph *db, int idx) {
  auto value = "arcane";
  try {
    for (int i = 0; i < point_per_thread; i++) {
      auto vertex_id = GetRandom(0, point_num - 1);
      for (int j = 0; j < edge_per_point; j++) {
        auto end_vertex_id = GetRandom(0, point_num - 1);
        auto txn = db->begin_transaction();
        txn.put_edge(vertex_id, 0, end_vertex_id, value);
        txn.commit(wait_visible);
        qps[idx].fetch_add(1);
      }
    }
  } catch (const std::exception &e) {
    std::cout << e.what() << std::endl;
  }
}

int main(int argc, char* argv[]) {
  const std::string block_path = "./livegraph_block";
  const std::string wal_path = "./livegraph_wal";
  livegraph::Graph graph(block_path, wal_path);
  std::vector<std::thread> workers;
  auto txn = graph.begin_batch_loader();
  for (int i = 0; i < point_num; i++) {
    txn.new_vertex();
  }
  txn.commit();
  for (int i = 0; i < thread_num; i++) {
    workers.push_back(std::thread([&](int idx) {
      Work(&graph, idx);
    }, i));
  }
  auto thread = std::thread([&]() {
    int64_t last = 0;
    while (true) {
      usleep(1000000);
      int64_t total = 0;
      for (int i = 0; i < thread_num; i++) {
        total += qps[i];
      }
      std::cout << "qps: " << total - last << std::endl;
      last = total;
    }
  });
  thread.join();
  return 0;
}