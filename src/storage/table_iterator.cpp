#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

// mode = false: begin iterator; mode = true: end iterator
TableIterator::TableIterator(bool mode, page_id_t first_page_id, Schema *schema, BufferPoolManager *buffer_pool_manager,
                             Transaction *txn, LogManager *log_manager, LockManager *lock_manager)
    : schema_(schema),
      buffer_pool_manager_(buffer_pool_manager),
      txn_(txn),
      log_manager_(log_manager),
      lock_manager_(lock_manager) {
    page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id));
    assert(page_ != nullptr);
    RowId rid;
    if (mode == BEGIN_ITERATOR) {
        page_->GetFirstTupleRid(&rid);
    } else {
        page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_->GetPrevPageId()));
        page_->GetFirstTupleRid(&rid);
        for (RowId next_rid = rid; page_->GetNextTupleRid(rid, &next_rid); rid = next_rid)
            ;
    }
    row_ = new Row(rid);
    page_->GetTuple(row_, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(first_page_id, false);
}

TableIterator::TableIterator(const TableIterator &other) { *this = other; }

TableIterator::~TableIterator() { delete row_; }

bool TableIterator::operator==(const TableIterator &itr) const { return row_->GetRowId() == itr.row_->GetRowId(); }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() { return *row_; }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        *row_ = *itr.row_;
        page_ = itr.page_;
        schema_ = itr.schema_;
        buffer_pool_manager_ = itr.buffer_pool_manager_;
        txn_ = itr.txn_;
        log_manager_ = itr.log_manager_;
        lock_manager_ = itr.lock_manager_;
    }
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
    RowId next_rid;
    if (page_->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
        page_->GetTuple(row_, schema_, txn_, lock_manager_);
        return *this;
    }
    page_id_t next_page_id = page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
        ASSERT(false, "No more tuples.");
        return *this;
    }
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    assert(page_ != nullptr);
    page_->GetFirstTupleRid(&next_rid);
    row_->SetRowId(next_rid);
    page_->GetTuple(row_, schema_, txn_, lock_manager_);
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator tmp(*this);
    operator++();
    return TableIterator(tmp);
}
