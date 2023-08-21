#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

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
  
  bpm_->NewPage(&header_page_id);

  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  /**Here, instead of maintaining the number of leaf page because of
   * potential performance bottlenack, we assume that header page always
   * stays in memory, such that we can get root page and reinterpret it 
   * to BPlusTreePage to get its size_ member.
  */ 
  //auto root_page_id = GetRootPageId();
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page_data->root_page_id_;
  BasicPageGuard root_page = bpm_->FetchPageBasic(root_page_id);
  auto* root_page_data = root_page.As<BPlusTreePage>();
  return (root_page_data->GetSize() == 0);
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
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {

  // If this B+ Tree is empty, then just return.  
  if(IsEmpty()){
    return false;
  }
  
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  auto probe_page_id = GetRootPageId();
  bool found = false;
  while(true){
	  // ReadPageGuard probe_page = bpm_->FetchPageRead(probe_page_id);
    BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  	const auto* probe_page_data = probe_page.As<BPlusTreePage>();

    if(probe_page_data->IsLeafPage()){
      const auto* leaf_data = probe_page.As<LeafPage>();
 			int index = leaf_data->SearchKey(key, comparator_);
			if(index != -1){
				result->push_back(leaf_data->ValueAt(index));
				found = true;
			}
			break;
    }
    const auto* internal_data = probe_page.As<InternalPage>(); 
		int index = internal_data->SearchKey(key, comparator_);		
    /**
     * Since SearchKey finds a minimum key that is greater than the target key
     * we should get its left pointer.
     */
    probe_page_id = internal_data->ValueAt(index - 1);
  }

  return found;
}

/*****************************************************************************
 * Insert
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.AsMut<BPlusTreeHeaderPage>();
  // Root page is invalided, which indicates the B+ tree is empty.
  if(header_page_data->root_page_id_ == INVALID_PAGE_ID){
    // If B+ tree is empty, we need allocate a new page for root.
    page_id_t new_page_id;
    bpm_->NewPageGuarded(&new_page_id);
    BasicPageGuard new_page = bpm_->FetchPageBasic(new_page_id);
    // The root page will be Leaf page.
    auto* new_page_data = new_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    new_page_data->Init();

    new_page_data->PlaceMapping(key, value, comparator_);
    // place a new mapping, the leaf page is dirty now.
    new_page.SetDirty();
    // update root page id
    header_page_data->root_page_id_ = new_page_id;

    return true;
  }

  // There are at least one node.
  page_id_t probe_page_id = header_page_data->root_page_id_;
  BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  auto* probe_page_data = probe_page.AsMut<BPlusTreePage>();

  // Another implementation.
  while(true){
    if(probe_page_data->IsLeafPage()){
      break;
    }
    auto* probe_internal_page = probe_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int index = probe_internal_page->SearchKey(key, comparator_);

    // Track the accessed pages
    ctx.basic_set_.push_back(std::move(probe_page));

    // The return value is the minimum greater than the target key.
    probe_page_id = probe_internal_page->ValueAt(index - 1);
    probe_page = bpm_->FetchPageBasic(probe_page_id);
    probe_page_data = probe_page.AsMut<BPlusTreePage>();
  }

  // After breaking, the probe_page_data is actually a BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

  auto* leaf_page_data = probe_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if(leaf_page_data->GetSize() < leaf_max_size_){
    // The leaf page need not split, so just insert
    leaf_page_data->PlaceMapping(key, value, comparator_);
    probe_page.SetDirty();

    while(!ctx.basic_set_.empty()){
      ctx.basic_set_.pop_back();
    }
  }else if(probe_page_id == header_page_data->root_page_id_){
    // Edge case: There is only node, which is root node and need split.

    // Step 1. Request a new page.
    page_id_t root_sibling_id;
    bpm_->NewPageGuarded(&root_sibling_id);
    BasicPageGuard root_sibling_page = bpm_->FetchPageBasic(root_sibling_id);
    auto* root_sibling_data = root_sibling_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    // Step 2. Split the overflow page.
    int divide_index = (leaf_max_size_ - 1)/2;
    for(int i = divide_index + 1; i < leaf_max_size_; i++){
      root_sibling_data->PlaceMapping(leaf_page_data->KeyAt(i), leaf_page_data->ValueAt(i), comparator_);
    }
    //leaf_page_data->IncreaseSize(leaf_max_size_ - divide_index - 1);
    leaf_page_data->SetSize(divide_index + 1);

    // Step 3. Insert new key-value
    if(comparator_(root_sibling_data->KeyAt(0), key) != 1){   // root_sibling_data->KeyAt(0) <= key
      root_sibling_data->PlaceMapping(key, value, comparator_);
    }else{
      leaf_page_data->PlaceMapping(key, value, comparator_);
    }

    // Step 4. Update the splited page metainfomation
    root_sibling_data->SetPageType(IndexPageType::LEAF_PAGE);
    root_sibling_data->SetNextPageId(leaf_page_data->GetNextPageId());
    leaf_page_data->SetNextPageId(root_sibling_id);
    probe_page.SetDirty();
    root_sibling_page.SetDirty();
    //leaf_page_data->SetSize(divide_index + 1);

    // Step 5. update root page. Get the right new page
    //auto new_internal_node = std::make_pair(leaf_page_data->KeyAt(leaf_page_data->GetSize() - 1), root_sibling_id);
    // root_sibling_data is leaf page
    auto new_internal_node = std::make_pair(root_sibling_data->KeyAt(0), root_sibling_id);

    page_id_t new_root_id;
    bpm_->NewPageGuarded(&new_root_id);
    BasicPageGuard root_page = bpm_->FetchPageBasic(new_root_id);
    auto* root_data = root_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    root_data->PlaceHead(header_page_data->root_page_id_);
    root_data->PlaceMapping(new_internal_node.first, new_internal_node.second, comparator_);
    header_page_data->root_page_id_ = new_root_id;
    root_page.SetDirty();

  }else{
    // The leaf page would split. Handle carefully.

    // Step 1. Request a new page
    page_id_t new_page_id;
    bpm_->NewPageGuarded(&new_page_id);
    BasicPageGuard new_page = bpm_->FetchPageBasic(new_page_id);
    auto* new_page_data = new_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    // Step 2. Split the overflow page. 
    // [0,divide_index] stay still, while [divide_point + 1, GetMaxSize-1] are put new leaf page.
    // Due to this division method, left page will be at least equal to the right page.
    //int divide_index = (leaf_page_data->GetMaxSize() - 1)/2;
    int divide_index = (leaf_max_size_ - 1)/2;
    for(int i = divide_index + 1; i < leaf_max_size_; i++){
      new_page_data->PlaceMapping(leaf_page_data->KeyAt(i), leaf_page_data->ValueAt(i), comparator_);
    }
    //leaf_page_data->IncreaseSize(leaf_max_size_ - divide_index -1);
    leaf_page_data->SetSize(divide_index + 1);

    // Step 3. insert new key-value.
    if(comparator_(new_page_data->KeyAt(0), key) != 1){     // new_page_data->KeyAt(0) <= key
      new_page_data->PlaceMapping(key, value, comparator_);
    }else{
      leaf_page_data->PlaceMapping(key, value, comparator_);
    }

    // Step 4. Update the split page metainfomation
    new_page_data->SetPageType(IndexPageType::LEAF_PAGE);
    new_page_data->SetNextPageId(leaf_page_data->GetNextPageId());
    leaf_page_data->SetNextPageId(new_page_id);
    probe_page.SetDirty();
    new_page.SetDirty();

    // Step 5. Update internal pages recurively with leaf_page_data and new_page_data
    // The division point is leaf_page_data[GetSize() - 1];
    auto new_internal_node = std::make_pair(new_page_data->KeyAt(0), new_page_id);
    bool root_splited = true;
    std::pair<KeyType, page_id_t> root_sibling;
    while(!ctx.basic_set_.empty()){
      BasicPageGuard tracked_internal_page(std::move(ctx.basic_set_.back()));
      ctx.basic_set_.pop_back();
      auto* tracked_internal_data = tracked_internal_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

      if(tracked_internal_data->GetSize() < internal_max_size_){
        root_splited = false;
        tracked_internal_data->PlaceMapping(new_internal_node.first, new_internal_node.second, comparator_);
        tracked_internal_page.SetDirty();
        // release all BasicPageGuard
        while(!ctx.basic_set_.empty()){
          ctx.basic_set_.pop_back();
        }
        break;
      }else{
        // Split the internal page. This is very similar to leaf page split.
        // Step 1. Request a new page.
        page_id_t new_page_id;
        bpm_->NewPageGuarded(&new_page_id);
        BasicPageGuard new_page = bpm_->FetchPageBasic(new_page_id);
        auto* new_page_data = new_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

        // Step 2. Split the overflow page.
        //int divide_index = tracked_internal_data->GetMaxSize()/2;
        int divide_index = internal_max_size_/2;

        // This head element is temporary.
        new_page_data->IncreaseSize(1);
        for(int i = divide_index + 1; i < internal_max_size_; i++){
          new_page_data->PlaceMapping(tracked_internal_data->KeyAt(i), tracked_internal_data->ValueAt(i), comparator_);
          tracked_internal_data->IncreaseSize(-1);
        }

        // Step 3. Insert new key-value
        if(comparator_(new_page_data->KeyAt(1), key) != 1){     // new_page_data->KeyAt(1) <= key
          new_page_data->PlaceMapping(new_internal_node.first, new_internal_node.second, comparator_);
        }else{
          tracked_internal_data->PlaceMapping(new_internal_node.first, new_internal_node.second, comparator_);
        }

        // Step 4. Update new_internal_node to be passed along the B+ Tree
        // division point is either tracked_interal_data[Getsize() - 1] or new_page_data[1]
        if(new_page_data->GetSize() >= tracked_internal_data->GetSize()){
          new_internal_node = std::make_pair(new_page_data->KeyAt(1), new_page_id);
          new_page_data->SetValueAt(0, new_page_data->ValueAt(1));
          for(int k = 1; k < new_page_data->GetSize() - 1; k++){
            new_page_data->SetKeyAt(k, new_page_data->KeyAt(k + 1));
            new_page_data->SetValueAt(k, new_page_data->ValueAt(k + 1));
          }
          new_page_data->IncreaseSize(-1);
        }else{
          new_internal_node = std::make_pair(tracked_internal_data->KeyAt(tracked_internal_data->GetSize() - 1), new_page_id);
          new_page_data->SetValueAt(0, tracked_internal_data->ValueAt(tracked_internal_data->GetSize() - 1));
          tracked_internal_data->IncreaseSize(-1);
        }
        
        //Set tracked_internal_data and new_page dirty
        tracked_internal_page.SetDirty();
        new_page.SetDirty();

        // Since every new_internal_node might be division node of root, we should keep it.
        root_sibling = new_internal_node;
      }
    }
    // The last step. Check whether the root is splited.
    // After basic_set_ is empty, it's possible that the root also need split.
    if(root_splited){
      page_id_t new_root_id;
      page_id_t old_root_id = header_page_data->root_page_id_;
      bpm_->NewPageGuarded(&new_root_id);
      BasicPageGuard new_root_page = bpm_->FetchPageBasic(new_root_id);
      auto* new_root_data = new_root_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      new_root_data->PlaceHead(old_root_id);
      new_root_data->PlaceMapping(root_sibling.first, root_sibling.second, comparator_); 
      new_root_page.SetDirty();
      // Update header page to new root page id.
      header_page_data->root_page_id_ = new_root_id;
    }
  }
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.AsMut<BPlusTreeHeaderPage>();
  if(header_page_data->root_page_id_ == INVALID_PAGE_ID){
    // The tree is empty, return immediately.
    return;
  }

  // Find the key along the tree
  page_id_t probe_page_id = header_page_data->root_page_id_;
  BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  auto* probe_page_data = probe_page.AsMut<BPlusTreePage>();
  while(true){
    if(probe_page_data->IsLeafPage()){
      break;
    }
    auto* probe_internal_page = probe_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int index = probe_internal_page->SearchKey(key, comparator_);

    // Track the accessed pages
    ctx.basic_set_.push_back(std::move(probe_page));

    probe_page_id = probe_internal_page->ValueAt(index - 1);
    probe_page = bpm_->FetchPageBasic(probe_page_id);
    probe_page_data = probe_page.AsMut<BPlusTreePage>();
  }

  auto* leaf_page_data = probe_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  int del_index = leaf_page_data->SearchKey(key, comparator_);
  leaf_page_data->Remove(del_index);
  probe_page.SetDirty();

  // Check if the deletion break the balance properity 
  if(leaf_page_data->GetSize() < leaf_max_size_/2){
    // Check if the leaf page is the root page.
    if(probe_page_id == header_page_data->root_page_id_){
      // Check if the deletion is the last node.
      if(leaf_page_data->GetSize() == 0){
        header_page_data->root_page_id_ = INVALID_PAGE_ID;
        header_page.SetDirty();
      }
    }else{
      // Handle edge case, the page is leaf page.
      BasicPageGuard tracked_internal_page(std::move(ctx.basic_set_.back()));
      ctx.basic_set_.pop_back();
      auto* tracked_internal_data = tracked_internal_page.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int key_idx = tracked_internal_data->SearchKey(key, comparator_) - 1;

      // Check if borrow from sibling.
      BasicPageGuard left_page;
      BasicPageGuard right_page;
      LeafPage* left_data = nullptr;
      LeafPage* right_data = nullptr;
      if(key_idx - 1 >= 0){
        page_id_t left_sibling_page_id = tracked_internal_data->ValueAt(key_idx - 1);
        left_page = bpm_->FetchPageBasic(left_sibling_page_id);
        left_data = left_page.AsMut<LeafPage>();
      } 
      if(key_idx + 1 <= internal_max_size_ - 1){
        page_id_t right_sibling_page_id = tracked_internal_data->ValueAt(key_idx + 1);
        right_page = bpm_->FetchPageBasic(right_sibling_page_id);
        right_data = right_page.AsMut<LeafPage>();
      }

      bool borrowed = false;
      if(left_data != nullptr || right_data != nullptr){
        // Probably can borrow from sibling page.
        // check if leaf sibling can be borrowed?
        if(left_data != nullptr && left_data->GetSize() > leaf_max_size_/2){
          // borrow from left sibling
          // TODO
          int size = left_data->GetSize();
          MappingType orphan = MappingType(left_data->KeyAt(size - 1), left_data->ValueAt(size - 1));
          leaf_page_data->PlaceMapping(orphan.first, orphan.second, comparator_);
          left_data->IncreaseSize(-1);
          tracked_internal_data->SetKeyAt(key_idx, orphan.first);

          // Set three pages dirty
          left_page.SetDirty();
          tracked_internal_page.SetDirty();
          probe_page.SetDirty();

          borrowed = true;
        }

        // 
        if(!borrowed && right_data != nullptr && right_data->GetSize() > leaf_max_size_/2){
          // borrow from right sibling   
          // TODO
          int size = right_data->GetSize();
          MappingType orphan = MappingType(right_data->KeyAt(0), right_data->ValueAt(0));
          leaf_page_data->PlaceMapping(orphan.first, orphan.second, comparator_);
          for(int i = 0; i < size - 1; i++){
            MappingType map = MappingType(right_data->KeyAt(i + 1), right_data->ValueAt(i + 1));
            right_data->SetMappingAt(i, map);
          }
          right_data->IncreaseSize(-1);
          tracked_internal_data->SetKeyAt(key_idx, right_data->KeyAt(0));

          // Set three page dirty
          right_page.SetDirty();
          tracked_internal_page.SetDirty();
          probe_page.SetDirty();

          borrowed = true;
        }
      }

      if(!borrowed){
        // Abosolutly can't borrow from sibling page. Two page should be merged.
        // Two cases: merge the leaf page with its left sibling
        //            merge the leaf page with its right sibling
        // Here, some issues we can concern about.
        // If the tree has more deletion than insertion, we should merge leaf page
        // with the one that has bigger size, and vice verse.
        //bool merged = false;
        if(left_data->GetSize() <= right_data->GetSize()){
          // Left sibling has bigger size. Merge it.
          const int key_index = tracked_internal_data->SearchKey(key, comparator_) - 1;
          int leaf_size = leaf_page_data->GetSize();
          int left_size = left_data->GetSize();

          for(int i = 0; i < leaf_size; i++){
            MappingType map(leaf_page_data->KeyAt(i), leaf_page_data->ValueAt(i));
            left_data->SetMappingAt(i + left_size, map);
          }
          for(int i = key_index; i < tracked_internal_data->GetSize() - 1; i++){
            std::pair<KeyType, page_id_t> map(tracked_internal_data->KeyAt(i + 1), tracked_internal_data->ValueAt(i + 1));
            tracked_internal_data->SetMappingAt(i, map);
          }

          left_data->IncreaseSize(leaf_size);
          left_page.SetDirty();
          leaf_page_data->IncreaseSize(-leaf_size);
          probe_page.SetDirty();
          tracked_internal_data->IncreaseSize(-1);
          tracked_internal_page.SetDirty();
        }else{
          // Right sibling has bigger size. Merge it.
          const int key_index = tracked_internal_data->SearchKey(key, comparator_) - 1;
          int leaf_size = leaf_page_data->GetSize();
          int right_size = right_data->GetSize();
          for(int i = 0; i < right_size; i++){
            MappingType map(right_data->KeyAt(i), right_data->ValueAt(i));
            leaf_page_data->SetMappingAt(i + leaf_size, map);
          }

          for(int i = key_index + 1; i < tracked_internal_data->GetSize() - 1; i++){
            std::pair<KeyType, page_id_t> map(tracked_internal_data->KeyAt(i + 1), tracked_internal_data->ValueAt(i + 1));
            tracked_internal_data->SetMappingAt(i, map);
          }

          leaf_page_data->IncreaseSize(right_size);
          probe_page.SetDirty();
          right_data->IncreaseSize(-right_size);
          right_page.SetDirty();
          tracked_internal_data->IncreaseSize(-1);
          tracked_internal_page.SetDirty();
        }

        // Recursively merge along the tree. This is the last step.
        BasicPageGuard probe_page = std::move(tracked_internal_page);
        auto* probe_data = tracked_internal_data;
        while(!ctx.basic_set_.empty() && probe_data->GetSize() < internal_max_size_/2){
          BasicPageGuard parent_page = std::move(ctx.basic_set_.back());
          ctx.basic_set_.pop_back();
          auto* parent_data = parent_page.AsMut<InternalPage>();
          const int key_index = parent_data->SearchKey(key, comparator_) - 1;
          BasicPageGuard left_page;
          BasicPageGuard right_page;
          InternalPage* left_data = nullptr;
          InternalPage* right_data = nullptr;
          if(key_index - 1 >= 0){
            page_id_t left_sibling_page_id = parent_data->ValueAt(key_index - 1);
            left_page = bpm_->FetchPageBasic(left_sibling_page_id);
            left_data = left_page.AsMut<InternalPage>();
          }

          if(key_index + 1 <= internal_max_size_ - 1){
            page_id_t right_sibling_page_id = parent_data->ValueAt(key_index + 1);
            right_page = bpm_->FetchPageBasic(right_sibling_page_id);
            right_data = right_page.AsMut<InternalPage>();
          }

          // try borrow from sibling,
          // if can't merge two internal page and then update probe_page and probe_data.

          bool internal_borrowed = false;
          if(left_data != nullptr || right_data != nullptr){
            if(left_data != nullptr && left_data->GetSize() > internal_max_size_ / 2){
              int size = left_data->GetSize();
              std::pair<KeyType, page_id_t> orphan(left_data->KeyAt(size - 1), left_data->ValueAt(size - 1));
              probe_data->PlaceMapping(parent_data->KeyAt(key_index), INVALID_PAGE_ID, comparator_);
              probe_data->SetValueAt(0, orphan.second);
              parent_data->SetKeyAt(key_index, orphan.first);
              left_data->IncreaseSize(-1);

              left_page.SetDirty();
              parent_page.SetDirty();
              probe_page.SetDirty();

              internal_borrowed = true;
            }

            if(!internal_borrowed && right_data != nullptr && right_data->GetSize() > internal_max_size_ / 2){
              int size = right_data->GetSize();
              // Notice this special orphan's value.
              std::pair<KeyType, page_id_t> orphan(right_data->KeyAt(1), right_data->ValueAt(0));
              probe_data->PlaceMapping(parent_data->KeyAt(key_index + 1), INVALID_PAGE_ID, comparator_);
              probe_data->SetValueAt(probe_data->GetSize() - 1, orphan.second);
              parent_data->SetKeyAt(key_index + 1, orphan.first);
              right_data->SetValueAt(0, right_data->ValueAt(1));
              for(int i = 1; i < size - 1; i++){
                std::pair<KeyType, page_id_t> map(right_data->KeyAt(i + 1), right_data->ValueAt(i + 1));
                right_data->SetMappingAt(i, map);
              }
              right_data->IncreaseSize(-1);
            
              probe_page.SetDirty();
              parent_page.SetDirty();
              right_page.SetDirty();
            
              internal_borrowed = true;
            }
          

          }
          probe_page = std::move(parent_page);
          probe_data = parent_data;
        }
      }

    }
  }
  return;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.As<BPlusTreeHeaderPage>();
  page_id_t probe_page_id = header_page_data->root_page_id_;
  BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  auto* probe_page_data = probe_page.AsMut<BPlusTreePage>();
  while(!probe_page_data->IsLeafPage()){
    auto* probe_page_internal_data = probe_page.AsMut<InternalPage>();
    page_id_t next_page_id = probe_page_internal_data->ValueAt(0);
    probe_page_id = next_page_id;
    probe_page = bpm_->FetchPageBasic(probe_page_id);
    probe_page_data = probe_page.AsMut<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(probe_page_id, 0, std::move(probe_page), bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.As<BPlusTreeHeaderPage>();
  page_id_t probe_page_id = header_page_data->root_page_id_;
  BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  auto* probe_page_data = probe_page.AsMut<BPlusTreePage>();
  while(!probe_page_data->IsLeafPage()){
    auto* probe_page_internal_data = probe_page.AsMut<InternalPage>();
    int idx = probe_page_internal_data->SearchKey(key, comparator_);
    page_id_t next_page_id = probe_page_internal_data->ValueAt(idx - 1);
    probe_page_id = next_page_id;
    probe_page = bpm_->FetchPageBasic(probe_page_id);
    probe_page_data = probe_page.AsMut<BPlusTreePage>();
  }
  auto* leaf_page_data = probe_page.AsMut<LeafPage>();
  int idx = leaf_page_data->SearchKey(key, comparator_);

  return INDEXITERATOR_TYPE(probe_page_id, idx, std::move(probe_page), bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.As<BPlusTreeHeaderPage>();
  page_id_t probe_page_id = header_page_data->root_page_id_;
  BasicPageGuard probe_page = bpm_->FetchPageBasic(probe_page_id);
  auto* probe_page_data = probe_page.AsMut<BPlusTreePage>();
  while(!probe_page_data->IsLeafPage()){
    auto* probe_page_internal_data = probe_page.AsMut<InternalPage>();
    page_id_t next_page_id = probe_page_internal_data->ValueAt(probe_page_internal_data->GetSize() - 1);
    probe_page_id = next_page_id;
    probe_page = bpm_->FetchPageBasic(probe_page_id);
    probe_page_data = probe_page.AsMut<BPlusTreePage>();
  }

  //auto* leaf_page_data = probe_page.AsMut<LeafPage>();
  return INDEXITERATOR_TYPE(probe_page_id, probe_page_data->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  // Get the B+ tree header page data from buffer pool manager.
  BasicPageGuard header_page = bpm_->FetchPageBasic(header_page_id_);
  auto* header_page_data = header_page.As<BPlusTreeHeaderPage>();
  // root_page_id_ is pulibc member in BPlusTreeHeaderPage
  return header_page_data->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
