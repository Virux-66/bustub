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

LRUKNode::LRUKNode(frame_id_t frame_id, size_t k):k_(k), fid_(frame_id){}

void LRUKNode::AddAccessRecord(size_t current_timestamp){
    if(history_.size() == k_){
        history_.pop_front();
    }
    history_.push_back(current_timestamp);
}

void LRUKNode::SetEvictable(bool is_evictable){
    is_evictable_ = is_evictable;
}

size_t LRUKNode::GetAccessNum(){
    return history_.size();
}

size_t LRUKNode::GetOldestTimeStamp(){
    return history_.front();
}

void LRUKNode::SetAccessType(AccessType access_type){
    last_access_type_ = access_type;
}

AccessType LRUKNode::GetAccessType(){
    return last_access_type_;
}

bool LRUKNode::IsEvictable(){
    return is_evictable_;
}



LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    SetCurrentTimeStamp();    
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if(curr_size_ == 0){
        return false;
    }
    std::unordered_map<frame_id_t, LRUKNode> candidate_less_k;
    std::unordered_map<frame_id_t, LRUKNode> candidate_great_equal_k;
    for(auto& itera: node_store_){
        if(itera.second.IsEvictable()){
            if(itera.second.GetAccessNum() < k_){
                candidate_less_k.insert(itera);
            }else{
                candidate_great_equal_k.insert(itera);
            }
        }
    }

    if(candidate_great_equal_k.empty() && candidate_less_k.empty()){
        return false;
    }

    if(candidate_less_k.empty()){
        size_t k_distance = 0;
        for(auto& itera: candidate_great_equal_k){
           if(current_timestamp_ - itera.second.GetOldestTimeStamp() > k_distance){
                k_distance =  current_timestamp_ - itera.second.GetOldestTimeStamp();
                //frame_id = new int(itera.first);
                *frame_id = itera.first;
           }
        }
    }else{
        size_t oldest_timestamp = ULONG_MAX;
        for(auto& itera: candidate_less_k){
            if(itera.second.GetOldestTimeStamp() < oldest_timestamp){
                oldest_timestamp = itera.second.GetOldestTimeStamp();
                //frame_id = new int(itera.first);
                *frame_id = itera.first;
            }
        }
    }
    {
        //  Critical section.
        std::lock_guard<std::mutex> lock(latch_);
        curr_size_ -= 1;        // Decrease the size of replacer and remove the frame's access history
        node_store_.erase(*frame_id);
    }
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    if(static_cast<long unsigned int>(frame_id) >= replacer_size_){
        throw "Invaild frame ID";
    }

    auto current = SetCurrentTimeStamp();

    if(node_store_.find(frame_id) == node_store_.end()){
        /**
         * This latch protects the insertion of node_store_
        */
        std::lock_guard<std::mutex> lock(latch_);
        LRUKNode node(frame_id, k_);
        //node.AddAccessRecord(current_timestamp_);
        node.AddAccessRecord(current);
        node.SetAccessType(access_type);
        node_store_.insert(std::make_pair(frame_id, node));
    }else{
        std::lock_guard<std::mutex> lock(latch_);
        auto& node = node_store_[frame_id];
        //node.AddAccessRecord(current_timestamp_);
        node.AddAccessRecord(current);
        node.SetAccessType(access_type);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(static_cast<long unsigned int>(frame_id) >= replacer_size_ || node_store_.find(frame_id) == node_store_.end()){
        throw "Invaild frame ID";
    }
    if(set_evictable){
        std::lock_guard<std::mutex> lock(latch_);
        curr_size_ += (node_store_[frame_id].IsEvictable()?0:1);
        node_store_[frame_id].SetEvictable(set_evictable);
    }else{
        std::lock_guard<std::mutex> lock(latch_);
        curr_size_ -= (node_store_[frame_id].IsEvictable()?1:0);
        node_store_[frame_id].SetEvictable(set_evictable);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if(static_cast<long unsigned int>(frame_id) >= replacer_size_ || node_store_.find(frame_id) == node_store_.end() ){
        throw "Invail frame ID";
    }
    if(!node_store_[frame_id].IsEvictable()){
        throw "The frame is non-evictable!";
    }
    std::lock_guard<std::mutex> lock(latch_);
    curr_size_ -= 1;
    node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_;
}

size_t LRUKReplacer::SetCurrentTimeStamp(){
    auto now = std::chrono::steady_clock::now(); 
    auto now_micrseconds = std::chrono::time_point_cast<std::chrono::microseconds>(now);    
    auto epoch = now_micrseconds.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::microseconds>(epoch);
    auto duration = static_cast<size_t>(value.count());
    {
        /** 
         * This is necessary because with multithreading cases,
         * multiple threads try to modify the current_timestamp.
        */ 
        std::lock_guard<std::mutex> lock(latch_timestamp_);
        current_timestamp_ = duration;
    }
    return duration;
}

}  // namespace bustub
