//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /**
   * @brief Remove the object that was accessed the least recently compared to
   *        all the elements being tracked
   *
   * @param frame_id
   * @return true
   * @return false
   */
  auto Victim(frame_id_t *frame_id) -> bool override;

  /**
   * @brief This method should be called after a page is pinned to a frame in the BufferPoolManager.
   *  It should remove the frame containing the pinned page from the LRUReplacer.
   *
   * @param frame_id
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * @brief This method should be called when the pin_count of a page becomes 0.
   * This method should add the frame containing the unpinned page to the LRUReplacer.
   *
   * @param frame_id
   */
  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  std::list<frame_id_t> dq;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> mp;
  size_t m_sz;

  std::mutex mu;
};

}  // namespace bustub
