/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int offset):
                                    page_id_(page_id),
                                    offset_(offset),
                                    current_(nullptr){}
                                    

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int offset, BasicPageGuard&& bpg, BufferPoolManager* bpm): 
                                    page_id_(page_id), 
                                    offset_(offset),
                                    bpg_(std::move(bpg)),
                                    bpm_(bpm){
    auto* leaf_page_data = bpg_.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(); 
    current_ = leaf_page_data->array_ + offset_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    bpg_.~BasicPageGuard();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    auto* leaf_page_data = bpg_.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if(leaf_page_data->GetNextPageId() == INVALID_PAGE_ID && offset_ == leaf_page_data->GetSize() - 1){
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool{
    return (page_id_ == itr.page_id_ && offset_ == itr.offset_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool{
    return (page_id_ != itr.page_id_ || offset_ != itr.offset_);
}


INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    return *current_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    auto* leaf_page_data = bpg_.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

    if(leaf_page_data->GetNextPageId() == INVALID_PAGE_ID && offset_ == leaf_page_data->GetSize() - 1){
        offset_ = leaf_page_data->GetSize();
        current_ = nullptr;
        bpg_.~BasicPageGuard();
    }else if(offset_ == leaf_page_data->GetSize() - 1){
        page_id_t next_page_id = leaf_page_data->GetNextPageId();
        BasicPageGuard next_page = bpm_->FetchPageBasic(next_page_id);
        auto* next_page_data = next_page.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
        page_id_ = next_page_id;
        offset_ = 0;
        current_ = &(next_page_data->array_[0]);
        bpg_.~BasicPageGuard();
        bpg_ = std::move(next_page);
    }else{
        offset_ += 1;
        current_ += 1;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
