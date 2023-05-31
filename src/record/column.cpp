#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
    ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
    switch (type) {
        case TypeId::kTypeInt:
            len_ = sizeof(int32_t);
            break;
        case TypeId::kTypeFloat:
            len_ = sizeof(float_t);
            break;
        default:
            ASSERT(false, "Unsupported column type.");
    }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
    ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
    uint32_t offset = 0;
    MACH_WRITE_UINT32(buf + offset, COLUMN_MAGIC_NUM);
    offset += sizeof(uint32_t);
    uint32_t len = name_.length();
    memcpy(buf + offset, &len, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(buf + offset, name_.c_str(), len);
    offset += len;
    MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(type_));
    offset += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, len_);
    offset += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, table_ind_);
    offset += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, nullable_);
    offset += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, unique_);
    offset += sizeof(uint32_t);
    return offset;
}

uint32_t Column::GetSerializedSize() const {
    uint32_t size = sizeof(uint32_t) * 7 + name_.length();
    return size;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    if (column != nullptr) {
        LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
        delete column;
    }
    uint32_t offset = 0;
    uint32_t magic = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    ASSERT(magic == COLUMN_MAGIC_NUM, "Invalid column magic number.");
    uint32_t nameLen = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    std::string name(buf + offset, nameLen);
    offset += nameLen;
    TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buf + offset));
    offset += sizeof(uint32_t);
    uint32_t len = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    uint32_t tableInd = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    bool nullable = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    bool unique = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    if (type == TypeId::kTypeChar) {
        column = new Column(name, type, len, tableInd, nullable, unique);
    } else {
        column = new Column(name, type, tableInd, nullable, unique);
    }
    return offset;
}
