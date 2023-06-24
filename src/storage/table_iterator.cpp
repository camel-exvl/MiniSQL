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
    if (mode == END_ITERATOR) {
        page_ = nullptr;
        row_ = new Row(RowId(INVALID_PAGE_ID, 0));
        return;
    }
    page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id));
    assert(page_ != nullptr);
    RowId rid;
    if (!page_->GetFirstTupleRid(&rid)) {   // empty table
        buffer_pool_manager_->UnpinPage(first_page_id, false);
        page_ = nullptr;
        row_ = new Row(RowId(INVALID_PAGE_ID, 0));
        return;
    }
    row_ = new Row(rid);
    page_->GetTuple(row_, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(first_page_id, false);
}

TableIterator::TableIterator(const TableIterator &other) { 
    row_ = new Row(*other.row_);
    page_ = other.page_;
    schema_ = other.schema_;
    buffer_pool_manager_ = other.buffer_pool_manager_;
    txn_ = other.txn_;
    log_manager_ = other.log_manager_;
    lock_manager_ = other.lock_manager_;
}

TableIterator::~TableIterator() { delete row_; }

bool TableIterator::operator==(const TableIterator &itr) const {
    if (page_ == nullptr || itr.page_ == nullptr) {
        return page_ == itr.page_;
    }
    return row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() { return *row_; }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        delete row_;
        row_ = new Row(*itr.row_);
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
    if (page_ == nullptr) {
        LOG(WARNING) << "TableIterator: operator++() on a null page";
        return *this;
    }
    RowId next_rid;
    if (page_->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
        row_->SetRowId(next_rid);
        page_->GetTuple(row_, schema_, txn_, lock_manager_);
        return *this;
    }
    page_id_t next_page_id = page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
        row_->SetRowId(RowId(INVALID_PAGE_ID, 0));
        page_ = nullptr;
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
