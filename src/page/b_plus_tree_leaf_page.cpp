#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetKeySize(key_size);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageId(page_id);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int l = 0, r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (KM.CompareKeys(KeyAt(mid), key) > 0) {
      r = mid - 1;
    } else if (KM.CompareKeys(KeyAt(mid), key) < 0) {
      l = mid + 1;
    } else {
      return mid;
    }
  }
  return l;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  return std::make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {  // key already exists
    LOG(WARNING) << "Duplicate key";
    return GetSize();
  }
  memmove(KeyAt(index + 1), KeyAt(index), (GetSize() - index) * pair_size);
  SetKeyAt(index, key);
  SetValueAt(index, value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int half = GetSize() >> 1;
  recipient->CopyNFrom(PairPtrAt(half), GetSize() - half);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  IncreaseSize(-1 * (GetSize() - half));
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(PairPtrAt(GetSize()), src, size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    value = ValueAt(index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (KM.CompareKeys(KeyAt(index), key) == 0) {
    if (index < GetSize() - 1) {
      memmove(KeyAt(index), KeyAt(index + 1), (GetSize() - index - 1) * pair_size);
    } else {
      memset(KeyAt(index), 0, pair_size);
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  // // update parent key
  // Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  // assert(parent != nullptr);
  // InternalPage *parent_page = reinterpret_cast<InternalPage *>(parent->GetData());
  // parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), KeyAt(1));
  // buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  memmove(KeyAt(0), KeyAt(1), (GetSize() - 1) * pair_size);
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  // // update parent key
  // Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  // assert(parent != nullptr);
  // InternalPage *parent_page = reinterpret_cast<InternalPage *>(parent->GetData());
  // parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), KeyAt(GetSize() - 1));
  // buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
  
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  memmove(KeyAt(1), KeyAt(0), GetSize() * pair_size);
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}