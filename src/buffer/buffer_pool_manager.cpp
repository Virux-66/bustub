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
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  Page *new_page = nullptr;
  if (!free_list_.empty()) {
    // (1). Pick the replacement frame from free_list_
    BUSTUB_ASSERT(!free_list_.empty(), "free_list_ must have available frame.");
    auto frame_id = free_list_.back();
    free_list_.pop_back();
    *page_id = AllocatePage();
    BUSTUB_ASSERT(*page_id >= 0, "page_id must be valid.");
    new_page = &pages_[frame_id];
    BUSTUB_ASSERT(page_table_.find(*page_id) == page_table_.end(), "New page_id should not exist in page_table.");
    BUSTUB_ASSERT(
        new_page->GetPageId() == INVALID_PAGE_ID && new_page->IsDirty() == false && new_page->GetPinCount() == 0,
        "New page should contains nothing.");

    new_page->page_id_ = *page_id;
    new_page->ResetMemory();
    new_page->pin_count_ = 1;

    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

  } else if (replacer_->Size() > 0) {
    // (2). Pick the replacement frame from the replacer.
    frame_id_t frame_id;
    auto evicted = replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(evicted == true, "At least one frame is evicted.");

    Page *evicted_page = &pages_[frame_id];
    page_id_t evicted_page_id = evicted_page->GetPageId();
    BUSTUB_ASSERT(page_table_.find(evicted_page_id) != page_table_.end(),
                  "The page evicted from the replacer must be in page table.");
    // Note: The page with frame_id must have 0 pin_count_ since one page with pin_count_ greater 0 is unevictable.
    BUSTUB_ASSERT(evicted_page->GetPinCount() == 0, "The evicted frame must have 0 pin count.");

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, evicted_page->GetData());
    }
    evicted_page->ResetMemory();
    page_table_.erase(evicted_page_id);

    *page_id = AllocatePage();
    BUSTUB_ASSERT(page_table_.find(*page_id) == page_table_.end(), "New page id should not exist in page table!");
    new_page = evicted_page;
    new_page->page_id_ = *page_id;
    new_page->pin_count_ = 1;
    new_page->is_dirty_ = false;

    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

  } else {
    BUSTUB_ASSERT(free_list_.empty() && replacer_->Size() == 0, "No available replacement frame.");
  }

  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  Page *target_page = nullptr;
  if (page_table_.find(page_id) != page_table_.end()) {
    // (1). The requested page exists in buffer pool.
    BUSTUB_ASSERT(page_table_.find(page_id) != page_table_.end(), "The requested page must be buffer pool!");

    auto target_frame_id = page_table_[page_id];
    target_page = &pages_[target_frame_id];
    target_page->pin_count_ += 1;

    replacer_->RecordAccess(target_frame_id);
    replacer_->SetEvictable(target_frame_id, false);
  } else if (!free_list_.empty()) {
    // (2). The requested page doesn't exist in buffer pool. Get a new page in free_list_ and place page from disk to
    // it.
    BUSTUB_ASSERT(page_table_.find(page_id) == page_table_.end(), "The requested page must not be buffer pool!");
    BUSTUB_ASSERT(!free_list_.empty(), "free_list_ must have available frame.");

    auto frame_id = free_list_.back();
    free_list_.pop_back();
    target_page = &pages_[frame_id];
    BUSTUB_ASSERT(target_page->GetPageId() == INVALID_PAGE_ID && target_page->IsDirty() == false &&
                      target_page->GetPinCount() == 0,
                  "New page should contains nothing.");

    target_page->ResetMemory();
    target_page->page_id_ = page_id;
    target_page->pin_count_ = 1;
    disk_manager_->ReadPage(page_id, target_page->data_);

    page_table_[page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  } else if (replacer_->Size() > 0) {
    // (3). The requested page doesn't exist in buffer pool and no available page in free_list_. Get a replacement frame
    // from replacer_.
    frame_id_t frame_id;
    auto evicted = replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(evicted == true, "At least one frame is evicted.");

    Page *evicted_page = &pages_[frame_id];
    page_id_t evicted_page_id = evicted_page->GetPageId();
    BUSTUB_ASSERT(page_table_.find(evicted_page_id) != page_table_.end(),
                  "The page evicted from the replacer must be in page table.");
    // Note: The page with frame_id must have 0 pin_count_ since one page with pin_count_ greater 0 is unevictable.
    BUSTUB_ASSERT(evicted_page->GetPinCount() == 0, "The evicted frame must have 0 pin count.");

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, evicted_page->GetData());
    }
    evicted_page->ResetMemory();
    page_table_.erase(evicted_page_id);

    target_page = evicted_page;

    disk_manager_->ReadPage(page_id, target_page->data_);
    target_page->page_id_ = page_id;
    target_page->pin_count_ = 1;
    target_page->is_dirty_ = false;

    page_table_[page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  } else {
    BUSTUB_ASSERT(page_table_.find(page_id) == page_table_.end() && free_list_.empty() && replacer_->Size() == 0,
                  "The requested page is not in buffer pool and there are no available pages to replace.");
  }
  return target_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  bool succ = false;
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto page = &pages_[frame_id];
    page->is_dirty_ |= is_dirty;
    if (page->GetPinCount() != 0) {
      BUSTUB_ASSERT(page->GetPinCount() > 0, "The pin_count must be greater than 0.");
      page->pin_count_ -= 1;
      auto is_evictable = (page->pin_count_ == 0);
      replacer_->SetEvictable(frame_id, is_evictable);
      succ = true;
    } else {
      BUSTUB_ASSERT(page->GetPinCount() == 0, "The pin count must be 0.");
      succ = false;
    }
  } else {
    BUSTUB_ASSERT(page_table_.find(page_id) == page_table_.end(), "The page doesn't exist in buffer pool.");
    succ = false;
  }
  return succ;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id cannot be INVALID_PAGE_ID.");
  bool succ = false;
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
    succ = true;
  } else {
    succ = false;
  }
  return succ;
}

void BufferPoolManager::FlushAllPages() {
  // Since FlushPage will get the lock, FlushAllPages should not get it.
  for (const auto &itera : page_table_) {
    auto succ = FlushPage(itera.first);
    BUSTUB_ASSERT(succ, "Existed pages must be able to flush out!");
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  bool succ = false;
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    BUSTUB_ASSERT(frame_id >= 0, "Invalid frame_id.");
    auto page = &pages_[frame_id];
    if (page->pin_count_ == 0) {
      BUSTUB_ASSERT(page_table_.find(page_id) != page_table_.end(), "The page must exist in page_table_");
      page_table_.erase(page_id);
      replacer_->Remove(frame_id);

      page->ResetMemory();
      page->page_id_ = INVALID_PAGE_ID;
      page->pin_count_ = 0;
      page->is_dirty_ = false;

      free_list_.push_back(frame_id);

      DeallocatePage(page_id);

      succ = true;
    }
  } else {
    succ = true;
  }
  return succ;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  BasicPageGuard guard(this, page);
  return guard;
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  ReadPageGuard guard(this, page);
  return guard;
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  WritePageGuard guard(this, page);
  return guard;
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto new_page = NewPage(page_id);
  BasicPageGuard guard(this, new_page);
  return guard;
}

}  // namespace bustub
