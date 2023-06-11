#include "primer/trie.h"
#include <memory>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if(root_ == nullptr){
	return nullptr;
  }
   
  auto probe = root_;
  for(auto ch: key){
    if(probe->children_.find(ch) != probe->children_.end()){
	  probe= probe->children_.find(ch)->second;
    }else{
      return nullptr;
	}
  }

  if(!(probe->is_value_node_)){
    return nullptr;
  }
  
  auto sp_value_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(probe);
  //assert(sp_value_node);  
  if(sp_value_node == nullptr){
	return nullptr;
  }
  return sp_value_node->value_.get();  
}


template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::unique_ptr<TrieNode> up_root;
  if(root_ == nullptr){
	up_root = std::make_unique<TrieNode>();
  }else{
	up_root = root_->Clone();
  }
  auto sp_root = std::shared_ptr<TrieNode>(std::move(up_root));
  auto probe = sp_root;
  auto sp_value = std::shared_ptr<T>(new T(std::move(value)));

  //empty key is also validated.
  if(key.empty()){
	auto children = sp_root->children_;
	sp_root = std::shared_ptr<TrieNode>(new TrieNodeWithValue(children, sp_value));
  }

  //for(auto& ch: key){
  for(std::size_t i =0; i<key.length(); i++){
	auto ch = key[i];
	auto target = probe->children_.find(ch);
	std::shared_ptr<TrieNode> sp_node;
	if(target == probe->children_.end()){
		//new created node to be inserted to children
		if(i == key.length() -1){
			//sp_node = std::make_shared<TrieNodeWithValue>(sp_value);
			sp_node = std::shared_ptr<TrieNode>(new TrieNodeWithValue(sp_value));
		}else{
			sp_node = std::make_shared<TrieNode>();
		}
		probe->children_.insert(std::pair(ch, sp_node));	
	}else{
		auto up_node = target->second->Clone();
		auto children = up_node->children_;
		//auto sp_node = std::shared_ptr<TrieNode>(std::move(up_node));
		if(i == key.length()-1){
			//sp_node = std::make_shared<TrieNodeWithValue>(sp_value);
			sp_node = std::shared_ptr<TrieNode>(new TrieNodeWithValue(children,sp_value));
		}else{
			sp_node = std::shared_ptr<TrieNode>(std::move(up_node));
		}
		probe->children_[ch] = sp_node;
	}
	probe = sp_node;

  } 

  Trie result(sp_root);
  return result;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto up_root = root_->Clone();
  auto sp_root = std::shared_ptr<TrieNode>(std::move(up_root)); 
  auto probe = sp_root;
  for(std::size_t i = 0; i < key.length(); i++){
	auto ch = key[i];
	auto target = probe->children_.find(ch);
	if(target == probe->children_.end()){
		break;
	}
	auto up_node = target->second->Clone();
	std::shared_ptr<TrieNode> sp_node(std::move(up_node));
	if(i == key.length() -1){
		if(sp_node->is_value_node_){
			if(sp_node->children_.empty()){
				probe->children_.erase(ch);
			}else{
				auto children = sp_node->children_;
				std::shared_ptr<TrieNode> node(new TrieNode(children));
				probe->children_[ch] = node;
			}
		}else{
			if(sp_node->children_.empty()){
				probe->children_.erase(ch);
			}else{
				probe->children_[ch] = sp_node;
			}
		}
	}else{
		probe->children_[ch] = sp_node;
		probe = sp_node;
	}
	

  }
  Trie result(sp_root);
  return result;
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
