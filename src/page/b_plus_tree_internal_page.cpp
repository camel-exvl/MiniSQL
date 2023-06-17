#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int l = 1, r = GetSize() - 1;
  // judge if the key is smaller than the first key
  // attention: if GetSize() == 1, then KeyAt(1) is invalid
  if (l <= r && KM.CompareKeys(KeyAt(1), key) > 0) {
    return ValueAt(0);
  }
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (KM.CompareKeys(KeyAt(mid), key) >= 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  if (l == GetSize()) { // the key is larger than all the keys in the page
    return ValueAt(l - 1);
  } else {
    return ValueAt(l);
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  IncreaseSize(2);
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index = ValueIndex(old_value);
  if (index == -1) {
    LOG(WARNING) << "The old value is not in the internal page.";
    return GetSize();
  }
  memmove(PairPtrAt(index + 2), PairPtrAt(index + 1), (GetSize() - index - 1) * pair_size);
  SetKeyAt(index + 1, new_key);
  SetValueAt(index + 1, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = GetSize() >> 1;
  int old_size = GetSize();
  recipient->CopyNFrom(PairPtrAt(half), old_size - half, buffer_pool_manager);
  SetSize(half);
  recipient->SetSize(old_size - half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  IncreaseSize(size);
  memmove(PairPtrAt(size), PairPtrAt(0), old_size * pair_size);
  memcpy(PairPtrAt(0), src, size * pair_size);
  for (int i = 0; i < size; i++) {
    page_id_t child_page_id = ValueAt(i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    assert(child_page != nullptr);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  memmove(PairPtrAt(index), PairPtrAt(index + 1), (GetSize() - index - 1) * pair_size);
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  IncreaseSize(-1);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  page_id_t parent_page_id = GetParentPageId();
  Page *parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
 
  SetKeyAt(0, middle_key);
  // update children's parent page id
  for (int i = 0; i < old_size; i++) {
    page_id_t child_page_id = ValueAt(i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    assert(child_page != nullptr);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  recipient->CopyNFrom(PairPtrAt(0), old_size, buffer_pool_manager);
  SetSize(0);
  parent_node->Remove(parent_node->ValueIndex(GetPageId()));
  buffer_pool_manager->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  // update children's parent page id
  page_id_t child_page_id = ValueAt(0);
  Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);

  // update parent's key
  SetKeyAt(0, middle_key);

  // update parent's children
  page_id_t parent_page_id = GetParentPageId();
  Page *parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // move first pair to the end of recipient
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  IncreaseSize(1);
  SetKeyAt(old_size, key);
  SetValueAt(old_size, value);
  
  Page *child_page = buffer_pool_manager->FetchPage(value);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  // update children's parent page id
  page_id_t child_page_id = ValueAt(old_size - 1);
  Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);

  // update parent's key
  SetKeyAt(old_size - 1, middle_key);

  // update parent's children
  page_id_t parent_page_id = GetParentPageId();
  Page *parent_page = buffer_pool_manager->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), KeyAt(old_size - 1));
  buffer_pool_manager->UnpinPage(parent_page_id, true);

  // move last pair to the front of recipient
  recipient->CopyFirstFrom(ValueAt(old_size - 1), buffer_pool_manager);
  Remove(old_size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  IncreaseSize(1);
  memmove(PairPtrAt(1), PairPtrAt(0), old_size * pair_size);
  SetValueAt(0, value);

  Page *child_page = buffer_pool_manager->FetchPage(value);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}