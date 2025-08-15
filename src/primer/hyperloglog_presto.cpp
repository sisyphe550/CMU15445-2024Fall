#include "primer/hyperloglog_presto.h"
#include <cmath>

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), n_leading_bits_(n_leading_bits) {
  constexpr auto k_hash_bits = static_cast<uint32_t>(sizeof(hash_t) * 8);

  int16_t bits = n_leading_bits;
  if (bits < 0) {
    bits = 0;
  }
  BUSTUB_ENSURE(bits < static_cast<int16_t>(k_hash_bits), "n_leading_bits out of range");

  n_leading_bits_ = bits;
  const size_t num_buckets = 1ULL << static_cast<uint64_t>(n_leading_bits_);
  dense_bucket_.assign(num_buckets, std::bitset<DENSE_BUCKET_SIZE>(0));
  overflow_bucket_.reserve(num_buckets);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);

  constexpr auto k_hash_bits = static_cast<uint32_t>(sizeof(hash_t) * 8);
  uint64_t bucket_idx;
  if (n_leading_bits_ == 0) {
    bucket_idx = 0;
  } else {
    bucket_idx =
        static_cast<uint16_t>(static_cast<uint64_t>(hash) >> (k_hash_bits - static_cast<uint32_t>(n_leading_bits_)));
  }

  const uint32_t max_rank = k_hash_bits - static_cast<uint32_t>(n_leading_bits_);
  uint32_t trailing_zeros = 0;
  if (hash == 0) {
    trailing_zeros = max_rank;
  } else {
    trailing_zeros = static_cast<uint32_t>(__builtin_ctzll(static_cast<uint64_t>(hash)));
    if (trailing_zeros > max_rank) {
      trailing_zeros = max_rank;
    }
  }

  const auto curr_dense = static_cast<uint32_t>(dense_bucket_[bucket_idx].to_ulong());
  uint32_t curr_overflow = 0;
  if (auto it = overflow_bucket_.find(bucket_idx); it != overflow_bucket_.end()) {
    curr_overflow = static_cast<uint32_t>(it->second.to_ulong());
  }
  const uint32_t curr_value = (curr_overflow << DENSE_BUCKET_SIZE) | curr_dense;

  if (trailing_zeros <= curr_value) {
    return;
  }

  const uint32_t low = trailing_zeros & ((1U << DENSE_BUCKET_SIZE) - 1U);
  dense_bucket_[bucket_idx] = std::bitset<DENSE_BUCKET_SIZE>(low);

  const uint32_t high = (trailing_zeros >> DENSE_BUCKET_SIZE) & ((1U << OVERFLOW_BUCKET_SIZE) - 1U);
  if (high != 0) {
    overflow_bucket_[bucket_idx] = std::bitset<OVERFLOW_BUCKET_SIZE>(high);
  } else {
    if (auto it = overflow_bucket_.find(bucket_idx); it != overflow_bucket_.end()) {
      overflow_bucket_.erase(it);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  const auto m = static_cast<uint64_t>(dense_bucket_.size());
  double acc = 0.0;

  for (size_t i = 0; i < m; i++) {
    const auto index = static_cast<uint16_t>(i);

    const auto dense_low = static_cast<uint32_t>(dense_bucket_[index].to_ulong());

    uint32_t overflow_high = 0;
    if (auto it = overflow_bucket_.find(index); it != overflow_bucket_.end()) {
      overflow_high = static_cast<uint32_t>(it->second.to_ulong());
    }

    const uint32_t curr_value = (overflow_high << DENSE_BUCKET_SIZE) | dense_low;

    acc += std::pow(2.0, -static_cast<double>(curr_value));
  }

  const double estimate = CONSTANT * static_cast<double>(m) * static_cast<double>(m) / acc;

  cardinality_ = static_cast<size_t>(std::floor(estimate));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
