#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
    lru_list_.clear();
    lru_set_.clear();
    max_size_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (lru_list_.empty()) {
        return false;
    }
    *frame_id = lru_list_.front();
    lru_list_.pop_front();
    lru_set_.erase(*frame_id);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    if (lru_set_.find(frame_id) != lru_set_.end()) {
        lru_list_.remove(frame_id);
        lru_set_.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (lru_set_.find(frame_id) == lru_set_.end()) {
        if (lru_list_.size() >= max_size_) {
            frame_id_t victim;
            Victim(&victim);
        }
        if (lru_list_.size() >= max_size_) {
            LOG(ERROR) << "LRUReplacer::Unpin: still no victim";
        } else {
            lru_list_.push_back(frame_id);
            lru_set_.insert(frame_id);
        }
    }
}

size_t LRUReplacer::Size() { return lru_list_.size(); }