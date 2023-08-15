//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
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
  SetSize(0);
  SetMaxSize(std::min(max_size,static_cast<int>(INTERNAL_PAGE_SIZE)));
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code

  if(index < 0){
    throw "Invailed array index";
  }

  KeyType key = array_[index].first;

  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if(index < 0){
    throw "Invailed array index";
  }
  array_[index].first = key;
  
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {

  if(index < 0){
    throw "Invailed array index";
  }

  ValueType value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, ValueType value) -> void{
  if(index < 0){
    throw "Invailed array index";
  } 
  array_[index].second = value;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PlaceHead(const ValueType& value) -> bool{
  array_[0].second = value;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PlaceMapping(const KeyType& key, const ValueType& value, KeyComparator comparator) -> bool{
  MappingType map = std::make_pair(key, value);
  
  // If the internal page is empty, just insert it.
  if(IsEmpty()){ 
    array_[1] = map;
    IncreaseSize(1);
    return true;
  }

  // binary search to find the appropriate insert position
  int l = 1;
  int r = GetSize() - 1;
  int pos = 1;  // The 0-th has invalided value.

  // The internal page has been full.
  //if(GetSize() == GetMaxSize()-1){
  if(GetSize() == GetMaxSize()){
    return false;
  }

  while(l <= r){
    int mid = (l + r)/2;
    if(l == r){
      if(comparator(array_[l].first, key) == -1){
        pos = l + 1;
      }else{
        pos = l;
      }
      break;
    }
    if(comparator(array_[mid].first, key) == -1){
      l = mid + 1;
    }else{
      r = mid -1;
    }
  }

  // Move elements back by one.
  for(int i = GetSize() - 1; i >= pos; i--){
    array_[i + 1] = array_[i];
  }

  // Insert new element.
  array_[pos] = map;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchKey(KeyType key, KeyComparator comparator) const -> int{
  // Internal page should start with 1.
  int l = 1;
  int r = GetSize() - 1;
  int pos = 1;

  while(l <= r){
    int mid = (l + r)/2;
    if(l == r){
      // If key is greatest among this internal page, pos will be GetSize() - 1 + 1 = GetSize()
      if(comparator(array_[l].first, key) != 1){
        pos = l + 1;        
      }else{
        pos = l;
      }
      break;
    }
    /*
    if(comparator(array_[mid].first, key) == -1){
      l = mid + 1;
    }else{
      r = mid - 1;
    }
    */
   if(comparator(array_[mid].first,key) == 1){
    r = mid - 1;
   }else{
    l = mid + 1;
   }
  }
  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetMappingAt(int index, const MappingType& map){
  if(index >= GetMaxSize()){
    return;
  }
  array_[index] = map;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
