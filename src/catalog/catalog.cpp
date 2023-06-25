#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size = 0;
  size += 4;  // magic num
  size += 4;  // table nums
  size += 4;  // index nums
  size += table_meta_pages_.size() * 8;
  size += index_meta_pages_.size() * 8;
  return size;
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    // init catalog meta
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {
    // read catalog
    auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    assert(catalog_page != nullptr);
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

    // read table
    for (auto iter : catalog_meta_->table_meta_pages_) {
      assert(LoadTable(0, iter.second) == DB_SUCCESS);
    }

    // read index
    for (auto iter : catalog_meta_->index_meta_pages_) {
      assert(LoadIndex(0, iter.second) == DB_SUCCESS);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // check if table name exists
  auto iter = table_names_.find(table_name);
  if (iter != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  // create table meta page
  page_id_t table_meta_page_id = INVALID_PAGE_ID;
  Page *table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  if (table_meta_page == nullptr) {
    return DB_FAILED;
  }
  table_id_t table_id = catalog_meta_->GetNextTableId();
  table_names_.emplace(table_name, table_id);
  catalog_meta_->table_meta_pages_.emplace(table_id, table_meta_page_id);

  // create table meta
  TableSchema *table_schema = TableSchema::DeepCopySchema(schema);  // 注意这里需要深拷贝，否则会有double free的问题
  auto table_heap = TableHeap::Create(buffer_pool_manager_, table_schema, nullptr, log_manager_, lock_manager_);
  auto *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), table_schema);
  table_meta->SerializeTo(table_meta_page->GetData());
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // update catalog meta
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page != nullptr);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto iter = table_names_.find(table_name);
  if (iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  return GetTable(iter->second, table_info);
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableNames(vector<string> &table_names) const {
  for (auto iter : table_names_) {
    table_names.push_back(iter.first);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // check if table name exists
  auto table_name_iter = table_names_.find(table_name);
  if (table_name_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  TableInfo *table_info = nullptr;
  if (GetTable(table_name_iter->second, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }

  // check if index name exists
  auto index_name_iter = index_names_.find(table_name);
  if (index_name_iter != index_names_.end()) {
    auto index_iter = index_name_iter->second.find(index_name);
    if (index_iter != index_name_iter->second.end()) {
      return DB_INDEX_ALREADY_EXIST;
    }
  }

  // check if index keys are valid
  for (auto key : index_keys) {
    uint32_t column_index;
    if (table_info->GetSchema()->GetColumnIndex(key, column_index) != DB_SUCCESS) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }

  // create index meta page
  page_id_t index_meta_page_id = INVALID_PAGE_ID;
  Page *index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  if (index_meta_page == nullptr) {
    return DB_FAILED;
  }
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  if (index_name_iter == index_names_.end()) {  // table does not exist
    std::unordered_map<std::string, index_id_t> index_name_map;
    index_name_map.emplace(index_name, index_id);
    index_names_.emplace(table_name, index_name_map);
  } else {  // table exists
    index_name_iter->second.emplace(index_name, index_id);
  }
  catalog_meta_->index_meta_pages_.emplace(index_id, index_meta_page_id);

  // create index meta
  std::vector<uint32_t> key_map;
  for (auto key : index_keys) {
    uint32_t key_index;
    table_info->GetSchema()->GetColumnIndex(key, key_index);
    if (key_index == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(key_index);
  }
  auto index_meta = IndexMetadata::Create(index_id, index_name, table_name_iter->second, key_map);
  index_meta->SerializeTo(index_meta_page->GetData());
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  // update catalog meta
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page != nullptr);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto table_name_iter = index_names_.find(table_name);
  if (table_name_iter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_name_iter = table_name_iter->second.find(index_name);
  if (index_name_iter == table_name_iter->second.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_name_iter->second;
  auto index_iter = indexes_.find(index_id);
  if (index_iter == indexes_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = index_iter->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto table_name_iter = index_names_.find(table_name);
  if (table_name_iter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  for (auto index_name_iter : table_name_iter->second) {
    auto index_iter = indexes_.find(index_name_iter.second);
    if (index_iter == indexes_.end()) {
      return DB_INDEX_NOT_FOUND;
    }
    indexes.push_back(index_iter->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // check if table name exists
  auto table_name_iter = table_names_.find(table_name);
  if (table_name_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  TableInfo *table_info = nullptr;
  if (GetTable(table_name_iter->second, table_info) != DB_SUCCESS) {
    return DB_FAILED;
  }

  // delete index if exists
  auto index_name_iter = index_names_.find(table_name);
  if (index_name_iter != index_names_.end()) {
    for (auto index_iter : index_name_iter->second) {
      if (DropIndex(table_name, index_iter.first) != DB_SUCCESS) {
        return DB_FAILED;
      }
    }
    index_name_iter->second.clear();
    index_names_.erase(table_name);
  }

  // delete table
  catalog_meta_->table_meta_pages_.erase(table_info->GetTableId());
  table_names_.erase(table_name);
  tables_.erase(table_info->GetTableId());
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // check if index name exists
  auto index_name_iter = index_names_.find(table_name);
  if (index_name_iter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id_iter = index_name_iter->second.find(index_name);
  if (index_id_iter == index_name_iter->second.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  // get index info and index meta
  auto index_id = index_id_iter->second;
  auto index_info_iter = indexes_.find(index_id);
  assert(index_info_iter != indexes_.end());
  auto index_info = index_info_iter->second;
  auto index_meta_iter = catalog_meta_->index_meta_pages_.find(index_id);
  assert(index_meta_iter != catalog_meta_->index_meta_pages_.end());
  auto index_meta_page_id = index_meta_iter->second;

  // delete index
  if (index_info->GetIndex()->Destroy() != DB_SUCCESS) {  // delete B+ tree
    return DB_FAILED;
  }
  if (!buffer_pool_manager_->DeletePage(index_meta_page_id)) {  // delete index meta page
    return DB_FAILED;
  }

  // index_name_iter->second.erase(index_name); // delete index name
  // if (index_name_iter->second.empty()) {
  //   index_names_.erase(table_name);
  // }
  indexes_.erase(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DeleteIndex(const string &table_name, const string &index_name) {
  auto index_name_iter = index_names_.find(table_name);
  if (index_name_iter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_name_iter->second.erase(index_name);
  if (index_name_iter->second.empty()) {
    index_names_.erase(table_name);
  }
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  bool res = buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  if (!res) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  assert(page != nullptr);
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetSchema(), nullptr, log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  table_names_.emplace(table_meta->GetTableName(), table_meta->GetTableId());
  tables_.emplace(table_meta->GetTableId(), table_info);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  auto index_page = buffer_pool_manager_->FetchPage(page_id);
  assert(index_page != nullptr);
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);
  // find table
  auto table_iter = catalog_meta_->table_meta_pages_.find(index_meta->GetTableId());
  assert(table_iter != catalog_meta_->table_meta_pages_.end());
  auto table_page = buffer_pool_manager_->FetchPage(table_iter->second);
  assert(table_page != nullptr);
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(table_page->GetData(), table_meta);
  buffer_pool_manager_->UnpinPage(table_iter->second, false);
  // create index info
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetSchema(), nullptr, log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  std::string index_name = index_info->GetIndexName();
  auto index_name_iter = index_names_.find(table_meta->GetTableName());
  if (index_name_iter == index_names_.end()) {
    std::unordered_map<std::string, index_id_t> index_name_map;
    index_name_map.emplace(index_name, index_meta->GetIndexId());
    index_names_.emplace(table_meta->GetTableName(), index_name_map);
  } else {  // table name exists
    index_name_iter->second.emplace(index_name, index_meta->GetIndexId());
  }
  indexes_.emplace(index_meta->GetIndexId(), index_info);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto table_iter = tables_.find(table_id);
  if (table_iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = table_iter->second;
  return DB_SUCCESS;
}