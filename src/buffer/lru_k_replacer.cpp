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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // return std::nullopt;
  std::lock_guard<std::mutex> lock(latch_);

  if (!new_frames_.empty()) {
    frame_id_t victim = new_frames_.back();

    new_frames_.pop_back();
    new_frames_iter_.erase(victim);
    node_store_.erase(victim);
    curr_size_--;

    return victim;
  }

  if (!cache_frames_.empty()) {
    frame_id_t victim = cache_frames_.back();

    cache_frames_.pop_back();
    cache_frames_locator_.erase(victim);
    node_store_.erase(victim);
    curr_size_--;

    return victim;
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
    bool in_new_frames = (new_frames_iter_.find(frame_id) != new_frames_iter_.end());
    bool in_cache_frames = (cache_frames_locator_.find(frame_id) != cache_frames_locator_.end());

    if (in_new_frames) {
      new_frames_.erase(new_frames_iter_[frame_id]);
      InsertIntoNewFrames(frame_id, it->second);

      if (it->second.history_.size() >= k_) {
        new_frames_.erase(new_frames_iter_[frame_id]);
        new_frames_iter_.erase(frame_id);
        InsertIntoCacheFrames(frame_id, it->second);
      }
    } else if (in_cache_frames) {
      cache_frames_.erase(cache_frames_locator_[frame_id]);

      InsertIntoCacheFrames(frame_id, it->second);
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

    if (new_frames_iter_.find(frame_id) != new_frames_iter_.end()) {
      new_frames_.erase(new_frames_iter_[frame_id]);
      new_frames_iter_.erase(frame_id);
    } else if (cache_frames_locator_.find(frame_id) != cache_frames_locator_.end()) {
      cache_frames_.erase(cache_frames_locator_[frame_id]);
      cache_frames_locator_.erase(frame_id);
    }
  } else if (!was_evictable && set_evictable) {
    curr_size_++;
    if (it->second.history_.size() < k_) {
      InsertIntoNewFrames(frame_id, it->second);
    } else {
      InsertIntoCacheFrames(frame_id, it->second);
    }
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
  if (new_frames_iter_.find(frame_id) != new_frames_iter_.end()) {
    new_frames_.erase(new_frames_iter_[frame_id]);
    new_frames_iter_.erase(frame_id);
  }
  if (cache_frames_locator_.find(frame_id) != cache_frames_locator_.end()) {
    cache_frames_.erase(cache_frames_locator_[frame_id]);
    cache_frames_locator_.erase(frame_id);
  }
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  // return 0;
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

void LRUKReplacer::InsertIntoCacheFrames(frame_id_t frame_id, const LRUKNode &node) {
  size_t distance = current_timestamp_ - node.history_.front();

  auto insert_pos = cache_frames_.begin();

  for (auto it = cache_frames_.begin(); it != cache_frames_.end(); ++it) {
    frame_id_t existing_frame = *it;
    auto &existing_node = node_store_[existing_frame];

    size_t existing_distance = current_timestamp_ - existing_node.history_.front();

    if (distance >= existing_distance) {
      insert_pos = std::next(it);
    } else {
      break;
    }
  }

  auto new_iter = cache_frames_.insert(insert_pos, frame_id);
  cache_frames_locator_[frame_id] = new_iter;
}

void LRUKReplacer::InsertIntoNewFrames(frame_id_t frame_id, const LRUKNode &node) {
  size_t distance = node.history_.front();

  auto insert_pos = new_frames_.begin();

  for (auto it = new_frames_.begin(); it != new_frames_.end(); ++it) {
    frame_id_t existing_frame = *it;
    auto &existing_node = node_store_[existing_frame];

    size_t existing_distance = existing_node.history_.front();

    if (distance <= existing_distance) {
      insert_pos = std::next(it);
    } else {
      break;
    }
  }

  auto new_iter = new_frames_.insert(insert_pos, frame_id);
  new_frames_iter_[frame_id] = new_iter;
}

}  // namespace bustub
