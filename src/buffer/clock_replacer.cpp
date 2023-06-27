#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {
    clock_list.clear();
    clock_status.clear();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    if (clock_list.empty()) {
        return false;
    }
    for (;;) {
        frame_id_t victim = clock_list.front();
        clock_list.pop_front();
        if (clock_status[victim] == 0) {
            *frame_id = victim;
            clock_status.erase(victim);
            return true;
        } else {
            clock_status[victim] = 0;
            clock_list.push_back(victim);
        }
    }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
    if (clock_status.find(frame_id) != clock_status.end()) {
        clock_status.erase(frame_id);
        clock_list.remove(frame_id);
    }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    if (clock_status.find(frame_id) == clock_status.end()) {
        if (clock_list.size() >= capacity) {
            frame_id_t victim;
            Victim(&victim);
        }
        if (clock_list.size() >= capacity) {
            LOG(ERROR) << "CLOCKReplacer::Unpin: still no victim";
        } else {
            clock_list.push_back(frame_id);
            clock_status[frame_id] = 1;
        }
    }
}

size_t CLOCKReplacer::Size() { return clock_list.size(); }