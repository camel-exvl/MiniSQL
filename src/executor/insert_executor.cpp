//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  table_heap_ = table_info_->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), indexes_);
  vector<Column *> columns = table_info_->GetSchema()->GetColumns();
  for(int i = 0; i < columns.size(); i++){
    if(columns[i]->IsUnique()){
      unique_columns_.emplace_back(i, columns[i]);
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row child_row{};
  RowId child_rid{};
  if (child_executor_->Next(&child_row, &child_rid)) {
    // check unique
    for(auto unique_column : unique_columns_){
      IndexInfo *index_info = nullptr;
      for (auto index : indexes_) {
        if (index->GetIndexKeySchema()->GetColumnCount() == 1 &&
            index->GetIndexKeySchema()->GetColumn(0)->GetName() == unique_column.second->GetName()) {
          index_info = index;
        }
      }
      assert(index_info != nullptr);
      std::vector<RowId> scan_result;
      std::vector<Field> key;
      key.emplace_back(*(child_row.GetField(unique_column.first)));
      if (index_info->GetIndex()->ScanKey(Row(key), scan_result, exec_ctx_->GetTransaction()) == DB_SUCCESS) {
        return false;
      }
    }
    if (table_heap_->InsertTuple(child_row, exec_ctx_->GetTransaction())) {
      child_rid = child_row.GetRowId();
      rid = &child_rid;
      // insert into index
      for (auto index : indexes_) {
        std::vector<Field> key;
        for (int i = 0; i < index->GetIndexKeySchema()->GetColumnCount(); i++) {
          uint32_t column_index;
          if (table_info_->GetSchema()->GetColumnIndex(index->GetIndexKeySchema()->GetColumn(i)->GetName(), column_index) != DB_SUCCESS) {
            return false;
          }
          key.emplace_back(*(child_row.GetField(column_index)));
        }
        index->GetIndex()->InsertEntry(Row(key), child_rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
    return false;
  } else {
    return false;
  }
}