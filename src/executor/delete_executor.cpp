//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  table_heap_ = table_info_->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row src_row{};
  RowId src_rid{};
  if (!child_executor_->Next(&src_row, &src_rid)) {
    return false;
  }
  table_heap_->MarkDelete(src_rid, exec_ctx_->GetTransaction());

  // update index
  for (auto &index_info : index_info_) {
    vector<Field> src_key;
    for (int i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
      uint32_t column_index;
      if (table_info_->GetSchema()->GetColumnIndex(index_info->GetIndexKeySchema()->GetColumn(i)->GetName(), column_index) != DB_SUCCESS) {
        return false;
      }
      src_key.emplace_back(*(src_row.GetField(column_index)));
    }
    if (index_info->GetIndex()->RemoveEntry(Row(src_key), src_rid, exec_ctx_->GetTransaction()) != DB_SUCCESS) {
      return false;
    }
  }
  return true;
}