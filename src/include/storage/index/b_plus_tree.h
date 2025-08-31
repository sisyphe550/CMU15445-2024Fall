/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

enum class Operation { INSERT = 0, DELETE };

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  void BatchOpsFromFile(const std::filesystem::path &file_name);
  
  /**
   * @brief 在一个内部节点中，根据给定的 key 查找并返回对应的子页面 ID。
   * 此函数假定页面已经是 BPlusTreeInternalPage 类型。
   *
   * @param key 要查找的 key。
   * @param page 指向当前内部节点的指针。
   * @return 应该继续向下搜索的子页面的 page_id。
   */
  auto FindLeafMidSearch(const KeyType &key, const InternalPage *page) const -> page_id_t;

  /**
  * @brief 在只读模式下，找到包含给定 key 的叶子节点。
  * 此函数不会修改任何页面，因此可以使用 Context 中的 read_set_来存储读取的页面，也可以选择节省开销而不使用。
  *
  * @param key 要查找的 key。
  * @return 包含给定 key 的叶子节点的读取页面保护。
  */
  auto FindLeafOnlyRead(const KeyType &key) -> std::optional<ReadPageGuard>;

  /**
  * @brief 用于在可能会修改节点的情况下，找到包含给定 key 的叶子节点
  * 当使用此函数时，插入或删除函数可能会发生溢出，因而发生修改需要进行写操作，故需要使用 Context 中的 write_set_ 来存储写入的页面。
  *
  * @param key 要查找的 key。
  * @param ctx 用于存储写入的页面。
  * @return 包含给定 key 的叶子节点的写入页面保护。
  */
  auto FindLeafMaybeWrite(const KeyType &key, Context *ctx, Operation op) -> std::optional<WritePageGuard>;

  /**
  * @brief 在查找目标节点时，如果路径中某一节点已经满了，则需要进行预分裂
  *
  * @param key 要插入的 key。
  * @param parent 父节点的写入页面保护。
  * @param child 子节点的写入页面保护。
  * @param ctx 用于存储写入的页面。
  * @return 预分裂后的父节点的写入页面保护。
  */
  auto PreSplitInternalChild(const KeyType &key, WritePageGuard &&parent, WritePageGuard &&child, Context *ctx) -> WritePageGuard;
  
  auto PreRebalanceChildForDelete(const KeyType &search_key, WritePageGuard parent_guard, WritePageGuard child_guard, Context *ctx) -> WritePageGuard;
 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
