//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  table_heap_ = table_info_->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row src_row{};
  RowId src_rid{};
  if (!child_executor_->Next(&src_row, &src_rid)) {  // 这里获取的是要更改的原始行
    return false;
  }
  Row updated_row = GenerateUpdatedTuple(src_row);
  RowId updated_rid = src_rid;
  table_heap_->UpdateTuple(updated_row, updated_rid, exec_ctx_->GetTransaction());

  // update index
  for (auto &index_info : index_info_) {
    vector<Field> src_key;
    vector<Field> updated_key;
    for (int i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
      uint32_t column_index;
      if (table_info_->GetSchema()->GetColumnIndex(index_info->GetIndexKeySchema()->GetColumn(i)->GetName(), column_index) != DB_SUCCESS) {
        return false;
      }
      src_key.emplace_back(*(src_row.GetField(column_index)));
      updated_key.emplace_back(*(updated_row.GetField(column_index)));
    }
    if (index_info->GetIndex()->RemoveEntry(Row(src_key), src_rid, exec_ctx_->GetTransaction()) != DB_SUCCESS) {
      return false;
    }
    if (index_info->GetIndex()->InsertEntry(Row(updated_key), updated_rid, exec_ctx_->GetTransaction()) != DB_SUCCESS) {
      return false;
    }
  }
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  vector<Field> fields;
  for (int i = 0; i < src_row.GetFieldCount(); i++) {
    fields.emplace_back(*(src_row.GetField(i)));
  }
  for (const auto &update_info : plan_->GetUpdateAttr()) {
    Field field = dynamic_pointer_cast<ConstantValueExpression>(update_info.second)->Evaluate(nullptr);
    fields[update_info.first] = field;
  }
  return Row(fields);
}