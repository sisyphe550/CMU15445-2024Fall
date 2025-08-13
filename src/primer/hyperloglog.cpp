#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) 
    : cardinality_(0), 
      n_bits_(n_bits),
      num_buckets_(0),
      registers_(),
      is_dirty_(true) {
  BUSTUB_ENSURE(n_bits_ >= 1 && n_bits_ < BITSET_CAPACITY, "n_bits out of range");
  num_buckets_ = static_cast<size_t>(1ULL << static_cast<unsigned>(n_bits));
  registers_.assign(num_buckets_, 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  // return {0};
  uint64_t hash_val = static_cast<uint64_t>(hash);
  std::bitset<BITSET_CAPACITY> bset(hash_val);
  return bset;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint64_t pos = 0;
  for (int i = BITSET_CAPACITY - 1 - n_bits_; i >= 0; --i) {
    if (bset[i]) {
      return pos;
    }
    ++pos;
  }
  return pos;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash);
  uint64_t pos = PositionOfLeftmostOne(bset) + 1;
  uint64_t hash_val = static_cast<uint64_t>(hash);
  uint64_t b = (hash_val >> (BITSET_CAPACITY - n_bits_)) & ((1ULL << n_bits_) - 1);
  registers_[b] = std::max<uint8_t>(registers_[b], static_cast<uint8_t>(pos));
  is_dirty_ = true;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double acc = 0.0;
  for (uint8_t r : registers_) {
    acc += std::pow(2, -r);
  }
  double est = CONSTANT * static_cast<double>(num_buckets_) * static_cast<double>(num_buckets_) / acc;
  cardinality_ = static_cast<size_t>(std::floor(est));
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
