//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  TableInfo *table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info);
  table_heap_ = table_info->GetTableHeap();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row child_row{};
  RowId child_rid{};
  if (child_executor_->Next(&child_row, &child_rid)) {
    // TODO: check constraint
    return table_heap_->InsertTuple(child_row, exec_ctx_->GetTransaction());
  } else {
    return false;
  }
}