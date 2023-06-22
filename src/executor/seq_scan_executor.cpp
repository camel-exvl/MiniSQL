//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  TableInfo *table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info);
  table_iter_ = new TableIterator(table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction()));
  end_iter_ = new TableIterator(table_info->GetTableHeap()->End());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  if (*table_iter_ == *end_iter_) {
    return false;
  }
  if (plan_->GetPredicate() == nullptr) { // no predicate
    *row = **table_iter_;
    *rid = (*table_iter_)->GetRowId();
    (*table_iter_)++;
    return true;
  }
  for (; *table_iter_ != *end_iter_; (*table_iter_)++) {
    *row = **table_iter_;
    *rid = (*table_iter_)->GetRowId();
    if (plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1))) {
      (*table_iter_)++;
      return true;
    }
  }
  return false;
}
