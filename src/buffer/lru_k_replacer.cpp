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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
// return std::nullopt; 
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t victim_frame = INVALID_FRAME_ID;
  size_t earliest_timestamp = std::numeric_limits<std::size_t>::max();
  
  for (auto& [frame_id, node] : node_store_) {
    if (!node.is_evictable_) continue;
    
    if (node.history_.size() < k_) {
      if (node.history_.empty()) {
        if (0 < earliest_timestamp) {
          node_store_.erase(frame_id);
          curr_size_--;
          return frame_id;
        }
      } 

      if (node.history_.front() < earliest_timestamp) {
        earliest_timestamp = node.history_.front();
        victim_frame = frame_id;
      }
    }
  }
  
  if (victim_frame != INVALID_FRAME_ID) {
    node_store_.erase(victim_frame);
    curr_size_--;
    return victim_frame;
  }
  
  size_t max_distance = 0;
  for (auto& [frame_id, node] : node_store_) {
    if (!node.is_evictable_) continue;

    if (node.history_.size() >= k_) {
      size_t distance = current_timestamp_ - node.history_.front();
      if (distance > max_distance) {
        max_distance = distance;
        victim_frame = frame_id;
      }
    }
  }

  if (victim_frame != INVALID_FRAME_ID) {
    node_store_.erase(victim_frame);
    curr_size_--;
    return victim_frame;
  }

  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id");
  }

  current_timestamp_++;

  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    it->second.history_.push_back(current_timestamp_);
    if (it->second.history_.size() > k_) {
      it->second.history_.pop_front();
    }
  } else {
    LRUKNode new_node;
    new_node.k_ = k_;
    new_node.fid_ = frame_id;
    new_node.is_evictable_ = false;
    new_node.history_.push_back(current_timestamp_);

    node_store_.emplace(frame_id, std::move(new_node));
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  bool was_evictable = it->second.is_evictable_;

  if (was_evictable == set_evictable) {
    return;
  }
  
  it->second.is_evictable_ = set_evictable;

  if (was_evictable && !set_evictable) {
    curr_size_--;
  } else if (!was_evictable && set_evictable) {
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    throw std::runtime_error("Cannot remove non-evictable frame");
  }

  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
  // return 0; 
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
