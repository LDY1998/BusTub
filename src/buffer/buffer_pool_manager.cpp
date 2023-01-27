//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  bool all_pinned = true;

  for (size_t i = 0; i < pool_size_; i++) {
    if (!pages_[i].pin_count_) {
      all_pinned = false;
      break;
    }
  }
  // if all pages are pinned, no new page can be created
  if (all_pinned) return nullptr;

  frame_id_t frame_id;
  if (free_list_.empty()) {
    bool is_evictable = replacer_->Evict(&frame_id);
    // no evictable page, no new page can be created
    if (!is_evictable) return nullptr;
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  Page *page_old = &pages_[frame_id];
  if (page_old->IsDirty()) {
    disk_manager_->WritePage(page_old->GetPageId(), page_old->GetData());
    page_old->is_dirty_ = false;
  }
  page_old->ResetMemory();
  page_table_->Remove(page_old->GetPageId());

  auto new_page_id = AllocatePage();
  page_table_->Insert(new_page_id, frame_id);
  Page *new_page = &pages_[frame_id];
  new_page->pin_count_++;
  new_page->page_id_ = new_page_id;
  new_page->is_dirty_ = false;

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  return new_page;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPage(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) return nullptr;
  frame_id_t frame_id;
  bool found = page_table_->Find(page_id, frame_id);
  if (found) return &pages_[frame_id];

  bool is_all_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (!pages_[i].pin_count_) {
      is_all_pinned = false;
      break;
    }
  }

  if (is_all_pinned) return nullptr;

  if (free_list_.empty()) {
    bool evictable = replacer_->Evict(&frame_id);
    if (!evictable) return nullptr;
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  Page *old_page = &pages_[frame_id];
  if (old_page->IsDirty()) {
    disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
    old_page->is_dirty_ = false;
  }

  old_page->ResetMemory();
  page_table_->Remove(old_page->GetPageId());

  Page *new_page = old_page;
  new_page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, new_page->GetData());
  page_table_->Insert(page_id, frame_id);

  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  return new_page;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  bool found = page_table_->Find(page_id, frame_id);
  if (!found || !pages_[frame_id].GetPinCount()) return false;
  Page *page = &pages_[frame_id];
  page->pin_count_--;
  if (!page->pin_count_) {
    replacer_->SetEvictable(page_id, true);
  }
  page->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id != INVALID_PAGE_ID) {
    std::unique_lock<std::mutex> lock(latch_);
    frame_id_t frame_id;
    bool found = page_table_->Find(page_id, frame_id);
    if (!found) return false;

    Page page = pages_[frame_id];
    disk_manager_->WritePage(page_id, page.GetData());
    page.is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].GetPageId());
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  bool found = page_table_->Find(page_id, frame_id);
  if (!found) return true;

  Page *page = &pages_[frame_id];
  if (page->GetPinCount()) return false;
  page_table_->Remove(page_id);
  page->ResetMemory();
  replacer_->Remove(frame_id);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
