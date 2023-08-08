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
  /**
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");
  */
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
  {
    /** We should acquire free_list_latch_ and latch_ at the same time to avoid dead lock.*/
    std::lock(latch_, free_list_latch_);
    std::lock_guard<std::mutex> lock1(latch_, std::adopt_lock);
    std::lock_guard<std::mutex> lock2(free_list_latch_, std::adopt_lock);

    if(!free_list_.empty()){
      frame_id_t frame_id = free_list_.front();
      free_list_.pop_front();
      Page* target_page = pages_ + frame_id;
      /** ResetPage might not be necessary if we reset page
       * each time when the page is put again to free list.
       */
      target_page->ResetPage();
      target_page->AddPinCount();
      /** Since next_page_id_ is std::atomic<page_id_t>, we do not need to protect it*/
      *page_id = AllocatePage(); 
      target_page->page_id_ = *page_id;
      /** bookkeeping in buffer_pool_manager*/
      page_table_.insert(std::make_pair(*page_id, frame_id));

      /** bookkeeping in replacer*/
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);

      return target_page;
    }
  }

  frame_id_t frame_id;
  auto found = replacer_->Evict(&frame_id);
  if(found){
    std::lock_guard<std::mutex> lock(latch_);
    page_id_t old_page_id = pages_[frame_id].GetPageId();
    Page* target_page = pages_ + frame_id;
    /** We should check whether the old page is dirty*/
    if(target_page->IsDirty()){
      /** Write old data to disk*/
      const char* old_data = target_page->GetData();
      disk_manager_->WritePage(old_page_id, old_data);
    }
    /** After writing dirty data to disk, we can reset this page.*/
    target_page->ResetPage();
    target_page->AddPinCount();

    *page_id = AllocatePage();
    target_page->page_id_ = *page_id;
    /** Here, we do not need to insert it in page_table_ because it has already been there.*/
    
    /** bookkeeping in replacer_*/
    /** the frame has been evicted before so RecordAccess creates a new entry.*/
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return target_page;
  }
  return nullptr; 
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  /** Check if the target page exists in memory*/
  {
    std::lock_guard<std::mutex> lock(latch_);
    if(page_table_.find(page_id) != page_table_.end()){
      auto frame_id = page_table_[page_id];
      Page* target_page = pages_ + frame_id;

      target_page->AddPinCount();
      replacer_->RecordAccess(frame_id, access_type);
      return target_page;
    }
  }

  /** Check if the free_list_ has available frame*/
  {
    std::lock(latch_, free_list_latch_);
    std::lock_guard<std::mutex> lock1(latch_, std::adopt_lock);
    std::lock_guard<std::mutex> lock2(free_list_latch_, std::adopt_lock);
    if(!free_list_.empty()){
      auto frame_id = free_list_.front();
      free_list_.pop_front();
      Page* target_page = pages_ + frame_id;

      target_page->ResetPage();
      // DEBUG: Should set page id for target_page
      target_page->ResetPageId(page_id);
      target_page->AddPinCount();

      /** Read data from disk to buffer pool*/
      disk_manager_->ReadPage(page_id, target_page->data_);
      /** bookkeeping in buffer_pool_manager_*/
      page_table_.insert(std::make_pair(page_id, frame_id));

      replacer_->RecordAccess(frame_id, access_type);
      replacer_->SetEvictable(frame_id, false);

      return target_page;
    }
  }

  frame_id_t frame_id;
  auto found = replacer_->Evict(&frame_id);
  if(found){
    std::lock_guard<std::mutex> lock(latch_);
    page_id_t old_page_id = pages_[frame_id].GetPageId();
    Page* target_page = pages_ + frame_id;
    /** We should check whether the old page is dirty*/
    if(target_page->IsDirty()){
      /** Write old data to disk*/
      const char* old_data = target_page->GetData();
      disk_manager_->WritePage(old_page_id, old_data);
    }

    /** After writing dirty data to disk, we can reset this page.*/
    target_page->ResetPage();
    // DEBUG: Should set page id for target_page
    target_page->ResetPageId(page_id);
    target_page->AddPinCount();

    target_page->page_id_ = page_id;
    /** Read data from disk to buffer pool*/
    disk_manager_->ReadPage(page_id, target_page->data_);

    /** Here, we do not need to insert it in page_table_ because it has already been there.*/

    /** bookkepping in replacer_*/
    /** the frame has been evicted before so RecorAccess creates a new entry.*/
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    
    return target_page;
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  {
    std::lock(latch_, free_list_latch_);
    std::lock_guard<std::mutex> lock1(latch_, std::adopt_lock);
    std::lock_guard<std::mutex> lock2(free_list_latch_, std::adopt_lock);

    if(page_table_.find(page_id) != page_table_.end()){
      auto frame_id = page_table_[page_id];
      Page* target_page = pages_ + frame_id;
      /** When its count is zero, this is unnormal situation. */
      if(target_page->GetPinCount() <= 0){
        return false; 
      }
      
      target_page->DecPinCount();
      target_page->is_dirty_ |= is_dirty;
      if(target_page->GetPinCount() == 0){
        /** Due to PinCount = 0, the frame should be evictable.*/
        replacer_->SetEvictable(frame_id, true);
        if(target_page->is_dirty_){
          disk_manager_->WritePage(page_id, target_page->data_);
          /** After flush to disk, we should reset dirty flag*/
          target_page->is_dirty_ = false;
        }  
        page_table_.erase(page_id);
        replacer_->Remove(frame_id);
        target_page->ResetPage();

        /** If the page is evicted, the frame should be added into free_list_*/
        free_list_.push_back(frame_id);
      }
      return true;
    }

    /** The page doesn't exist in buffer pool: unnormal situation.*/
    return false;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  if(page_table_.find(page_id) == page_table_.end()){
    return false; 
  }
  {
    std::lock_guard<std::mutex> lock1(latch_);
    auto frame_id = page_table_[page_id];
    Page* target_page = pages_ + frame_id;
    if(target_page->IsDirty()){
      disk_manager_->WritePage(page_id, target_page->data_);
    }
    target_page->is_dirty_ = false;
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for(auto itera: page_table_){
    FlushPage(itera.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  if(page_table_.find(page_id) == page_table_.end()){
    return true;
  }

  auto frame_id = page_table_[page_id];
  Page* target_page = pages_ + frame_id;

  if(target_page->GetPinCount() != 0){
    return false;
  }  

  {
    std::lock(latch_, free_list_latch_);
    std::lock_guard<std::mutex> lock1(latch_, std::adopt_lock);
    std::lock_guard<std::mutex> lock2(free_list_latch_, std::adopt_lock);

    if(target_page->is_dirty_){
      disk_manager_->WritePage(page_id, target_page->data_);
    }

    page_table_.erase(page_id);
    target_page->ResetPage();
    replacer_->Remove(frame_id);

    free_list_.push_front(frame_id);
  }
  return true; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  Page* target_page = FetchPage(page_id);
  return {this, target_page};
  //return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  Page* target_page = FetchPage(page_id);
  // Should not get the lock here. 
  /*
  if(target_page != nullptr){
    target_page->rwlatch_.RLock();
  }
  */
  return {this, target_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  Page* target_page = FetchPage(page_id);
  /*
  if(target_page != nullptr){
    target_page->rwlatch_.WLock();
  }
  */
  return {this, target_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page* target_page = NewPage(page_id);
  return {this, target_page};
}

}  // namespace bustub
