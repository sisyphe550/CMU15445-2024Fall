#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  //return true; 
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  // Context ctx;
  // (void)ctx;
  auto guard_opt = FindLeafOnlyRead(key);
  if (!guard_opt.has_value()) {
    return false;
  } 
  auto guard = std::move(guard_opt.value());
  auto leaf_page = guard.template As<LeafPage>();
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf_page->KeyAt(mid), key);
    if (cmp == 0) {
      result->push_back(leaf_page->ValueAt(mid));
      return true;
    }
    if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // return false;

  // 持有头页写闩
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;

  // 空树：直接创建叶子根
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t leaf_pid = bpm_->NewPage();
    auto leaf_guard = bpm_->WritePage(leaf_pid);
    auto leaf = leaf_guard.AsMut<LeafPage>();
    leaf -> Init(leaf_max_size_);
    leaf -> SetNextPageId(INVALID_PAGE_ID);

    leaf -> SetKeyAt(0, key);
    leaf -> SetValueAt(0, value);
    leaf -> SetSize(1);

    header -> root_page_id_ = leaf_pid;
    return true;
  }

  // 正常路径：找到目标叶子（内部节点在路径上已被按需分裂：若叶子将满，父闩已在 ctx.write_set_ 中）
  auto leaf_opt = FindLeafMaybeWrite(key, &ctx, Operation::INSERT);
  if (!leaf_opt.has_value()) {
    return false;
  }
  auto leaf_guard = std::move(leaf_opt.value());
  auto leaf = leaf_guard.template AsMut<LeafPage>();

  // 叶子内二分定位 & 重复键检查
  int n = leaf -> GetSize();
  int lo = 0, hi = n - 1, pos = 0;
  bool found = false;
  while (lo <= hi) {
    int mid = lo + (hi - lo) / 2;
    int cmp = comparator_(leaf -> KeyAt(mid), key);
    if (cmp == 0) {
      found = true;
      pos = mid;
      break;
    }
    if (cmp < 0) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (found) {
    return false; // 重复键
  }
  pos = lo; // 插入位置

  // 叶子不满，就地插入并返回
  if (n < leaf_max_size_) {
    for (int i = n; i > pos; --i) {
      leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
      leaf->SetValueAt(i, leaf->ValueAt(i - 1));
    }
    leaf->SetKeyAt(pos, key);
    leaf->SetValueAt(pos, value);
    leaf->SetSize(n + 1);
    return true;
  }

  // 叶子满，需要分裂
  page_id_t new_leaf_pid = bpm_->NewPage();
  auto new_leaf_guard = bpm_->WritePage(new_leaf_pid);
  auto new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf -> Init(leaf_max_size_);
  new_leaf -> SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_pid);

  KeyType middle_key{};

  leaf->MoveHalfTo(new_leaf, middle_key);

  if (comparator_(key, middle_key) >= 0) {
    // 插入右侧（新叶）
    int m = new_leaf->GetSize();
    int lo2 = 0, hi2 = m - 1, p2 = 0;
    while(lo2 <= hi2) {
      int mid2 = lo2 + (hi2 - lo2) / 2;
      int cmp2 = comparator_(new_leaf->KeyAt(mid2), key);
      if (cmp2 < 0) {
        lo2 = mid2 + 1;
      } else {
        hi2 = mid2 - 1;
      }
    }
    p2 = lo2;
    for (int i = m; i > p2; --i) {
      new_leaf->SetKeyAt(i, new_leaf->KeyAt(i - 1));
      new_leaf->SetValueAt(i, new_leaf->ValueAt(i - 1));
    }
    new_leaf->SetKeyAt(p2, key);
    new_leaf->SetValueAt(p2, value);
    new_leaf->SetSize(m + 1);
  } else {
    // 插入左侧（当前叶）
    int ln = leaf->GetSize();
    int lo2 = 0, hi2 = ln - 1, p2 = 0;
    while (lo2 <= hi2) {
      int mid2 = lo2 + (hi2 - lo2) / 2;
      int cmp2 = comparator_(leaf->KeyAt(mid2), key);
      if (cmp2 < 0) {
        lo2 = mid2 + 1;
      } else {
        hi2 = mid2 - 1;
      }
    }
    p2 = lo2;
    for (int i = ln; i > p2; --i) {
      leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
      leaf->SetValueAt(i, leaf->ValueAt(i - 1));
    }
    leaf->SetKeyAt(p2, key);
    leaf->SetValueAt(p2, value);
    leaf->SetSize(ln + 1);
  }

  // 将middle_key + new_leaf_pid 插入父节点
  page_id_t left_child_pid = leaf_guard.GetPageId();
  page_id_t right_child_pid = new_leaf_guard.GetPageId();

  // 如果没有父（叶子原来就是根），需要新建根
  if (ctx.write_set_.empty()) {
    page_id_t new_root_pid = bpm_->NewPage();
    auto new_root_guard = bpm_->WritePage(new_root_pid);
    auto new_root = new_root_guard.AsMut<InternalPage>();
    new_root->Init(internal_max_size_);
    new_root->SetSize(1);
    new_root->SetValueAt(0, left_child_pid);
    new_root->InsertNodeAfter(left_child_pid, middle_key, right_child_pid);
    header->root_page_id_ = new_root_pid;
    return true;
  }

  // 有父，在父中插入
  auto parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent = parent_guard.AsMut<InternalPage>();
  parent->InsertNodeAfter(left_child_pid, middle_key, right_child_pid);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  //return 0;
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafMidSearch(const KeyType &key, const InternalPage *page) const -> page_id_t {
  // 1. Using binary search
  int left = 1;
  int right = page->GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(key, page->KeyAt(mid)) >= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return page->ValueAt(left - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafOnlyRead(const KeyType &key) -> std::optional<ReadPageGuard> {
  page_id_t current_page_id = GetRootPageId();
  if (current_page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  auto guard = bpm_->ReadPage(current_page_id);
  // Loop search subpages
  while (true) {
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      return std::move(guard);
    }

    auto internal_page = guard.As<InternalPage>();
    page_id_t next_page_id = FindLeafMidSearch(key, internal_page);
    auto child_guard = bpm_->ReadPage(next_page_id);

    guard.Drop();
    guard = std::move(child_guard);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafMaybeWrite(const KeyType &key, Context *ctx, Operation op) -> std::optional<WritePageGuard> {
  BUSTUB_ASSERT(ctx != nullptr, "ctx must not be nullptr");

  auto parent = bpm_->WritePage(ctx->root_page_id_);
  if (op == Operation::INSERT) {
    auto p_bpt = parent.AsMut<BPlusTreePage>();
    if (!p_bpt->IsLeafPage()) {
      auto p_int = parent.AsMut<InternalPage>();
      if (p_int->GetSize() == internal_max_size_) {
        auto header = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
        page_id_t old_root = parent.GetPageId();

        page_id_t new_root_id = bpm_->NewPage();
        auto new_root_guard = bpm_->WritePage(new_root_id);
        auto new_root = new_root_guard.AsMut<InternalPage>();
        new_root->Init(internal_max_size_);
        new_root->SetSize(1);
        new_root->SetValueAt(0, old_root);

        page_id_t right_id = bpm_->NewPage();
        auto right_guard = bpm_->WritePage(right_id);
        auto right_page = right_guard.AsMut<InternalPage>();
        right_page->Init(internal_max_size_);

        KeyType mid{};
        p_int->MoveHalfTo(right_page, mid);
        new_root->InsertNodeAfter(old_root, mid, right_id);

        header->root_page_id_ = new_root_id;
        ctx->root_page_id_ = new_root_id;

        parent.Drop();
        parent = std::move(new_root_guard);
        right_guard.Drop();
      }
    }
  }

  while (true) {
    auto parent_bpt = parent.As<BPlusTreePage>();
    if (parent_bpt -> IsLeafPage()) {
      return std::move(parent);
    }

    auto parent_internal = parent.As<InternalPage>();
    page_id_t child_id = FindLeafMidSearch(key, parent_internal);

    auto child = bpm_->WritePage(child_id);
    auto child_bpt = child.As<BPlusTreePage>();

    bool need_parent = false;

    if (op == Operation::INSERT) {
      if (!child_bpt->IsLeafPage()) {
        auto child_internal = child.As<InternalPage>();
        if (child_internal->GetSize() == internal_max_size_) {
          child = PreSplitInternalChild(key, std::move(parent), std::move(child), ctx);
          parent = std::move(ctx->write_set_.back());
          child_bpt = child.As<InternalPage>();
        }
        need_parent = false;
      } else {
        auto child_leaf = child.As<LeafPage>();
        need_parent = (child_leaf->GetSize() == leaf_max_size_);
      }
    } else if (op == Operation::DELETE) {
      if (child_bpt->IsLeafPage()) {
        auto child_leaf = child.As<LeafPage>();
        if (child_leaf->GetSize() == child_leaf->GetMinSize()) {
          child = PreRebalanceChildForDelete(key, std::move(parent), std::move(child), ctx);
          parent = std::move(ctx->write_set_.back());
          need_parent = false;
        } else {
          need_parent = true;
        }
      } else {
        auto child_internal = child.As<InternalPage>();
        if (child_internal->GetSize() == child_internal->GetMinSize()) {
          child = PreRebalanceChildForDelete(key, std::move(parent), std::move(child), ctx);
          parent = std::move(ctx->write_set_.back());
          need_parent = false;
        } else {
          need_parent = false;
        }
      }
    }

    if (need_parent) {
      ctx->write_set_.emplace_back(std::move(parent));
    } else {
      parent.Drop();
    }
    parent = std::move(child);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PreSplitInternalChild(const KeyType &key, WritePageGuard &&parent, WritePageGuard &&child,
                                           Context *ctx) -> WritePageGuard {
  auto parent_page = parent.template AsMut<InternalPage>();
  auto full_child_page = child.template AsMut<InternalPage>();

  page_id_t new_internal_pid;
  {
    new_internal_pid = bpm_ -> NewPage();
    auto new_guard = bpm_ -> WritePage(new_internal_pid);
    auto new_internal_page = new_guard.AsMut<InternalPage>();
    new_internal_page -> Init();

    KeyType middle_key{};
    full_child_page->MoveHalfTo(new_internal_page, middle_key);

    parent_page->InsertNodeAfter(child.GetPageId(), middle_key, new_internal_pid);

    bool go_right = (comparator_(key, middle_key) >= 0);
    if (go_right) {
      child.Drop();
      child = std::move(new_guard);
    } else {
      new_guard.Drop();
    }
  }

  ctx->write_set_.emplace_back(std::move(parent));

  return std::move(child);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PreRebalanceChildForDelete(const KeyType &search_key, WritePageGuard parent_guard, WritePageGuard child_guard, Context *ctx) -> WritePageGuard {
  return parent_guard;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
