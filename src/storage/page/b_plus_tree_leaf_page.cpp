//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(std::min(max_size, static_cast<int>(LEAF_PAGE_SIZE)));
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code

  if(index < 0){
    throw "Invailed arrary index";
  }

  KeyType key = array_[index].first;

  return key;
}

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code

  if(index < 0){
    throw "Invailed arrary index";
  }

  ValueType value = array_[index].second;
  
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PlaceMapping(const KeyType& key, const ValueType& value, KeyComparator comparator) -> bool{
  MappingType map = std::make_pair(key, value);
  
  // If this page is a new page, just insert mapping
  if(IsEmpty()){
    array_[0] = map;
    IncreaseSize(1);
    return true;
  }

  // binary search to find the appropriate 1
  int l = 0;
  int r = GetSize() - 1;
  int pos = 0;

  // The leaf page has been full.
  if(GetSize() == GetMaxSize()){
    return false;
  }

  while(l <= r){
    int mid = (l + r)/2;
    if(comparator(array_[mid].first, key) == 0){
      pos = mid;
      break;
    }
    if(comparator(array_[mid].first, key) == -1){
      pos = mid + 1;
      l = mid + 1;
    }else{
      if(mid < pos){
        pos = mid;
      }
      r = mid - 1;
    }
  }

  for(int i = GetSize() - 1; i >= pos; i--){
    array_[i + 1] = array_[i];
  }
  array_[pos] = map;
  IncreaseSize(1);
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetMappingAt(int index, const MappingType& map){
  if(index >= GetMaxSize()){
    return;
  }
  array_[index] = map;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchKey(KeyType key, KeyComparator comparator) const -> int{
  // Leaf page shoud start with zero 0.
  int l = 0;
  int r = GetSize() - 1;
  int pos = -1;
  while(l <= r){
    int mid = (l + r)/2;

    if(comparator(array_[mid].first, key) == 0){
      pos = mid;
      break;
    }

    if(comparator(array_[mid].first, key) == -1){
      l = mid + 1;
    }else{
      r = mid - 1;
    }
  }

  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int offset) -> bool{
  if(offset >= GetSize()){
    return false;
  }
  for(int i = offset; i < GetSize() - 1; i++){
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1); 
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
