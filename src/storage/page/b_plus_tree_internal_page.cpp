//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { 
  //return {}; 
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  key_array_[index] = key;  
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}
/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  //return 0; 
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  page_id_array_[index] = value;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(ValueType old_value, const KeyType &middle_key, ValueType new_value) {
  int idx = ValueIndex(old_value);

  int insert_pos = idx + 1;
  int size = GetSize();

  for (int i = size; i > insert_pos; i--) {
    page_id_array_[i] = page_id_array_[i - 1];
  }
  for (int i = size; i > insert_pos; i--) {
    if (i >= 1) {
      key_array_[i] = key_array_[i - 1];
    }
  }

  page_id_array_[insert_pos] = new_value;
  key_array_[insert_pos] = middle_key;

  SetSize(size + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, KeyType &middle_key) {
  int size = GetSize();

  int move_size = ceil(size / 2);
  middle_key = KeyAt(move_size);

  int j = 0;

  recipient -> SetSize(0);
  recipient -> SetMaxSize(GetMaxSize());

  recipient -> page_id_array_[j++] =  page_id_array_[move_size];
  for (int i = move_size + 1; i < size; i++) {
    recipient -> key_array_[j] = key_array_[i];
    recipient -> page_id_array_[j] = page_id_array_[i];
    j++;
  }
  recipient -> SetSize(j);

  SetSize(move_size);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
