//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : m_sz(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  LOG_DEBUG("Finding Victim");
  if (dq.empty()) {
    return false;
  }
  std::unique_lock<std::mutex> lock(mu);
  auto id = dq.front();
  LOG_DEBUG("Victim frame id: %d", id);
  *frame_id = id;
  dq.pop_front();
  mp.erase(id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  LOG_DEBUG("Pinning frame id: %d", frame_id);

  if (mp.find(frame_id) != mp.end()) {
    std::unique_lock<std::mutex> lock(mu);
    auto id_iter = mp[frame_id];
    dq.erase(id_iter);
    mp.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  LOG_DEBUG("Unpin frame id: %d", frame_id);
  if (mp.find(frame_id) == mp.end()) {
    if (Size() >= m_sz) {
      int val;
      Victim(&val);
    }
    std::unique_lock<std::mutex> lock(mu);
    dq.push_back(frame_id);
    mp[frame_id] = --dq.end();
  }
}

auto LRUReplacer::Size() -> size_t { return dq.size(); }

}  // namespace bustub
