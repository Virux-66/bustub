#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    if(this == &that){
        return;
    }
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    /** Actually, I don't pretty understand what Drop means*/
    bpm_->UnpinPage(page_->GetPageId(),  page_->IsDirty());
    bpm_ = nullptr;
    page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    if(this == &that){
        return *this;
    }
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;

    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    if(bpm_ && page_){
        Drop();
    }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
    if(this == &that){
        return;
    }
    guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    if(this == &that){
        return *this;
    }
    guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    guard_.page_->RUnlatch();
    guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
    Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept{
    if(this == &that){
        return;
    }
    guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard &{
    if(this == &that){
        return *this;
    }
    guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    guard_.page_->WUnlatch();
    guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    Drop();
}  // NOLINT

}  // namespace bustub
