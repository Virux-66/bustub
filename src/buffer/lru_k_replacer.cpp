//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

void LRUKNode::AddRecord() {
  auto now = std::chrono::steady_clock::now();
  auto duration = now.time_since_epoch();
  auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if (history_.size() >= k_) {
    // history has been full. Remove one before adding a new timestamp.
    history_.pop_front();
    history_.push_back(static_cast<size_t>(microseconds));
    BUSTUB_ASSERT(history_.size() == k_, "LRUKNode size unexpected!");
  } else {
    history_.push_back(static_cast<size_t>(microseconds));
  }
}

auto LRUKNode::GetFrameId() -> frame_id_t { return fid_; }

void LRUKNode::SetEvictable(bool is_evictable) { is_evictable_ = is_evictable; }

auto LRUKNode::GetEvictable() const -> bool { return is_evictable_; }

auto LRUKNode::GetBackward(size_t k) const -> size_t {
  size_t backward_k = 0;
  if (history_.size() < k) {
    backward_k = std::numeric_limits<size_t>::max();
  } else {
    size_t index = 0;
    for (auto itera = history_.rbegin(); itera != history_.rend(); ++itera) {
      index += 1;
      if (index == k) {
        backward_k = *itera;
        break;
      }
    }
  }
  return backward_k;
}

auto LRUKNode::GetHistorySize() const -> size_t { return history_.size(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    // no evictable pages exist.
    return false;
  }

  *frame_id = SelectEvictableNode();

  BUSTUB_ASSERT(0 <= *frame_id && *frame_id <= static_cast<int32_t>(replacer_size_), "A evictable node must exist!");
  // thread-safe. Multiple threads might choose the evictable node.
  // But only one thread can take off the LRUKNode.
  if (node_store_.find(*frame_id) != node_store_.end()) {
    node_store_.erase(*frame_id);
    curr_size_ -= 1;
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  BUSTUB_ASSERT(0 <= frame_id && frame_id <= static_cast<int32_t>(replacer_size_), "Invalid frame_id!");

  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node(k_, frame_id, false);
    node_store_.insert({frame_id, node});
  }
  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "The LRUKNode of the frame_id must exist!");
  node_store_[frame_id].AddRecord();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  BUSTUB_ASSERT(0 <= frame_id && frame_id <= static_cast<int32_t>(replacer_size_), "Invalid frame_id!");
  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "The LRUKNode must exist!");

  if (node_store_[frame_id].GetEvictable() ^ set_evictable) {
    node_store_[frame_id].SetEvictable(set_evictable);
    curr_size_ += (set_evictable ? 1 : -1);
  }
  BUSTUB_ASSERT(node_store_[frame_id].GetEvictable() == set_evictable, "Unexpected Evitable status");
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(node_store_.find(frame_id) != node_store_.end(), "The frame must exist!");
  BUSTUB_ASSERT(node_store_[frame_id].GetEvictable(), "The frame should be evictable.");

  node_store_.erase(frame_id);
  curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::SelectEvictableNode() -> frame_id_t {
  frame_id_t frame_id = -1;  // the selected frame
  bool inf_node = false;     // Is there a infinite distance node in node_stone_
  size_t least_recent_time = std::numeric_limits<size_t>::max();

  for (const auto &itera : node_store_) {  // Select a node to evict
    if (!itera.second.GetEvictable()) {
      continue;
    }
    BUSTUB_ASSERT(itera.second.GetEvictable(), "LRUKNode should be evictable!");
    if (itera.second.GetHistorySize() < k_ && !inf_node) {
      frame_id = itera.first;
      least_recent_time = itera.second.GetBackward(itera.second.GetHistorySize());
      inf_node = true;
    } else if (itera.second.GetHistorySize() < k_ && inf_node) {
      if (itera.second.GetBackward(itera.second.GetHistorySize()) < least_recent_time) {
        frame_id = itera.first;
        least_recent_time = itera.second.GetBackward(itera.second.GetHistorySize());
      }
      BUSTUB_ASSERT(inf_node, "A infinite LRUKNode should have already existed!");
    } else if (itera.second.GetHistorySize() == k_ && !inf_node) {
      if (itera.second.GetBackward(k_) < least_recent_time) {
        frame_id = itera.first;
        least_recent_time = itera.second.GetBackward(k_);
      }
    } else if (itera.second.GetHistorySize() == k_ && inf_node) {
      BUSTUB_ASSERT(frame_id != -1, "A infinite LRUKNode should have already existed!");
      continue;
    } else {
      BUSTUB_ASSERT(0, "Unexpected Situation!");
    }
  }
  BUSTUB_ASSERT(frame_id != -1, "The frame_id must be greater than zero.");
  return frame_id;
}

}  // namespace bustub
