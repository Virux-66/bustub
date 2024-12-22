#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if(page_ == nullptr || bpm_ == nullptr){
    return;
  }
  BUSTUB_ASSERT(page_ != nullptr, "The page_ must be nullptr.");
  BUSTUB_ASSERT(bpm_ != nullptr, "The bpm_ must be nullptr.");

  auto page_id = page_->GetPageId();
  bpm_->UnpinPage(page_id, is_dirty_);
  // The following assert can't be guarantee due to this function might be called
  // when the pin count of page is 0.
  // BUSTUB_ASSERT(succ == true, "Unpin op must be successful.");
  BUSTUB_ASSERT(page_->GetPinCount() >= 0, "The pin count of page must be greater than or equal 0.");
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if(this != &that){
    Drop();

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this; 
}

BasicPageGuard::~BasicPageGuard(){
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
  guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if(this != &that){
    guard_.page_->RUnlatch();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if(guard_.page_ == nullptr || guard_.bpm_ == nullptr){
    return;
  }
  BUSTUB_ASSERT(guard_.page_ != nullptr, "The page_ must be nullptr.");
  BUSTUB_ASSERT(guard_.bpm_ != nullptr, "The bpm_ must be nullptr.");

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept{
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if(this != &that){
    guard_.page_->WUnlatch();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if(guard_.page_ == nullptr || guard_.bpm_ == nullptr){
    return;
  }
  BUSTUB_ASSERT(guard_.page_ != nullptr, "The page_ must be nullptr.");
  BUSTUB_ASSERT(guard_.bpm_ != nullptr, "The bpm_ must be nullptr.");

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
