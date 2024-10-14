#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> cur = root_;
  for (const auto &ch: key) {
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second;
  }

  auto val_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  return val_node == nullptr ? nullptr : val_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (key.size() == 0) {
    return Trie(std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value))));
  }

  std::shared_ptr<TrieNode> new_root(root_->Clone());
  auto cur = new_root;

  for (auto i = 0ul; i < key.size(); i++) {
    std::shared_ptr<TrieNode> new_node;

    auto it = cur->children_.find(key[i]);
    if (it != cur->children_.end()) {
      auto copy_node = it->second->Clone();
      new_node = i == key.size() - 1
                 ? std::make_shared<TrieNodeWithValue<T>>(copy_node->children_, std::make_shared<T>(std::move(value)))
                 : std::shared_ptr<TrieNode>(std::move(copy_node));
    } else {
      new_node = i == key.size() - 1
                 ? std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))
                 : std::make_shared<TrieNode>();
    }

    cur->children_[key[i]] = new_node;
    cur = new_node;
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (key.size() == 0) {
    if (!root_->is_value_node_) {
      return Trie(root_);
    }

    auto new_root = std::make_shared<TrieNode>(root_->children_);
    return Trie(new_root);
  }

  auto cur = root_;
  std::vector<std::shared_ptr<const TrieNode>> path;
  for (const auto &ch: key) {
    auto it = cur->children_.find(ch);
    // not such key
    if (it == cur->children_.end()) {
      return Trie(root_);
    }

    path.emplace_back(cur);
    cur = it->second;
  }
  path.emplace_back(cur);

  // not such key
  if (!cur->is_value_node_) {
    return Trie(root_);
  }

  // find the last non_empty node
  int i = path.size() - 1;
  auto is_empty = cur->children_.empty();
  while (is_empty && i > 0) {
    i -= 1;
    is_empty = !path[i]->is_value_node_ && path[i]->children_.size() == 1;
  }

  if (i == 0) {
    // if root is empty, return empty trie
    if (is_empty) {
      return Trie();
    }

    // else remove key[0] child
    std::shared_ptr<TrieNode> new_root(root_->Clone());
    new_root->children_.erase(key[0]);
    return Trie(new_root);
  }

  std::shared_ptr<TrieNode> new_root(root_->Clone());
  auto cur_node = new_root;
  for (int j = 0; j < i - 1; j++) {
    auto child = cur_node->children_[key[j]];
    auto new_node = std::shared_ptr<TrieNode>(child->Clone());
    cur_node->children_[key[j]] = new_node;
    cur_node = new_node;
  }

  std::shared_ptr<TrieNode> new_node;
  if (i == int(path.size() - 1)) {
    new_node = std::make_shared<TrieNode>(cur_node->children_[key[i-1]]->children_);
  } else {
    new_node = std::shared_ptr<TrieNode>(cur_node->children_[key[i-1]]->Clone());
    new_node->children_.erase(key[i]);
  }

  cur_node->children_[key[i-1]] = new_node;
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
