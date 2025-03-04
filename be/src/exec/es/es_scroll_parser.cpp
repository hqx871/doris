// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/es/es_scroll_parser.h"

#include <gutil/strings/substitute.h>

#include <boost/algorithm/string.hpp>
#include <string>

#include "common/status.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/mem_tracker.h"
#include "util/string_parser.hpp"
#include "vec/common/string_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_ID = "_id";

// get the original json data type
std::string json_type_to_string(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNumberType:
        return "Number";
    case rapidjson::kStringType:
        return "Varchar/Char";
    case rapidjson::kArrayType:
        return "Array";
    case rapidjson::kObjectType:
        return "Object";
    case rapidjson::kNullType:
        return "Null Type";
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
        return "True/False";
    default:
        return "Unknown Type";
    }
}

// transfer rapidjson::Value to string representation
std::string json_value_to_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> temp_writer(scratch_buffer);
    value.Accept(temp_writer);
    return scratch_buffer.GetString();
}

static const std::string ERROR_INVALID_COL_DATA =
        "Data source returned inconsistent column data. "
        "Expected value of type {} based on column metadata. This likely indicates a "
        "problem with the data source library.";
static const std::string ERROR_MEM_LIMIT_EXCEEDED =
        "DataSourceScanNode::$0() failed to allocate "
        "$1 bytes for $2.";
static const std::string ERROR_COL_DATA_IS_ARRAY =
        "Data source returned an array for the type $0"
        "based on column metadata.";
static const std::string INVALID_NULL_VALUE =
        "Invalid null value occurs: Non-null column `$0` contains NULL";

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type)                              \
    do {                                                                     \
        if (col.IsArray()) {                                                 \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document slice is : " << json_value_to_string(col);     \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col, type)                         \
    do {                                                                     \
        if (!col.IsNumber()) {                                               \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document value is: " << json_value_to_string(col);      \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type)                       \
    do {                                                                        \
        if (result != StringParser::PARSE_SUCCESS) {                            \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                     \
    do {                                                                 \
        std::stringstream ss;                                            \
        ss << "Expected value of type: " << type_to_string(type)         \
           << "; but found type: " << json_type_to_string(col.GetType()) \
           << "; Document slice is : " << json_value_to_string(col);     \
        return Status::RuntimeError(ss.str());                           \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                            bool pure_doc_value) {
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col[0], type);
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    if (sizeof(T) < 16) {
        *reinterpret_cast<T*>(slot) = v;
    } else {
        DCHECK(sizeof(T) == 16);
        memcpy(slot, &v, sizeof(v));
    }

    return Status::OK();
}

template <typename T>
static Status get_float_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                              bool pure_doc_value) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    *reinterpret_cast<T*>(slot) = v;

    return Status::OK();
}

template <typename T>
static Status insert_float_value(const rapidjson::Value& col, PrimitiveType type,
                                 vectorized::IColumn* col_ptr, bool pure_doc_value, bool nullable) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&v)), 0);

    return Status::OK();
}

template <typename T>
static Status insert_int_value(const rapidjson::Value& col, PrimitiveType type,
                               vectorized::IColumn* col_ptr, bool pure_doc_value, bool nullable) {
    if (col.IsNumber()) {
        T value = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col[0], type);
        T value = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&v)), 0);

    return Status::OK();
}

ScrollParser::ScrollParser(bool doc_value_mode) : _size(0), _line_index(0) {}

ScrollParser::~ScrollParser() = default;

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    // rely on `_size !=0 ` to determine whether scroll ends
    _size = 0;
    _document_node.Parse(scroll_result.c_str(), scroll_result.length());
    if (_document_node.HasParseError()) {
        return Status::InternalError("Parsing json error, json is: {}", scroll_result);
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll response:" << scroll_result;
        return Status::InternalError("Document has not a scroll id field");
    }

    if (!exactly_once) {
        const rapidjson::Value& scroll_node = _document_node[FIELD_SCROLL_ID];
        _scroll_id = scroll_node.GetString();
    }
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value& outer_hits_node = _document_node[FIELD_HITS];
    // if has no inner hits, there has no data in this index
    if (!outer_hits_node.HasMember(FIELD_INNER_HITS)) {
        return Status::OK();
    }
    const rapidjson::Value& inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
    }
    _inner_hits_node.CopyFrom(inner_hits_node, _document_node.GetAllocator());
    // how many documents contains in this batch
    _size = _inner_hits_node.Size();
    return Status::OK();
}

int ScrollParser::get_size() const {
    return _size;
}

const std::string& ScrollParser::get_scroll_id() {
    return _scroll_id;
}

Status ScrollParser::fill_columns(const TupleDescriptor* tuple_desc,
                                  std::vector<vectorized::MutableColumnPtr>& columns,
                                  bool* line_eof,
                                  const std::map<std::string, std::string>& docvalue_context) {
    *line_eof = true;

    if (_size <= 0 || _line_index >= _size) {
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    bool pure_doc_value = false;
    if (obj.HasMember("fields")) {
        pure_doc_value = true;
    }
    const rapidjson::Value& line = obj.HasMember(FIELD_SOURCE) ? obj[FIELD_SOURCE] : obj["fields"];

    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];
        auto col_ptr = columns[i].get();

        if (!slot_desc->is_materialized()) {
            continue;
        }
        if (slot_desc->col_name() == FIELD_ID) {
            // actually this branch will not be reached, this is guaranteed by Doris FE.
            if (pure_doc_value) {
                return Status::RuntimeError("obtain `_id` is not supported in doc_values mode");
            }
            // obj[FIELD_ID] must not be NULL
            std::string _id = obj[FIELD_ID].GetString();
            size_t len = _id.length();

            col_ptr->insert_data(const_cast<const char*>(_id.data()), len);
            continue;
        }

        const char* col_name = pure_doc_value ? docvalue_context.at(slot_desc->col_name()).c_str()
                                              : slot_desc->col_name().c_str();

        rapidjson::Value::ConstMemberIterator itr = line.FindMember(col_name);
        if (itr == line.MemberEnd() && slot_desc->is_nullable()) {
            auto nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);
            nullable_column->insert_data(nullptr, 0);
            continue;
        } else if (itr == line.MemberEnd() && !slot_desc->is_nullable()) {
            std::string details = strings::Substitute(INVALID_NULL_VALUE, col_name);
            return Status::RuntimeError(details);
        }

        const rapidjson::Value& col = line[col_name];

        PrimitiveType type = slot_desc->type().type;

        // when the column value is null, the subsequent type casting will report an error
        if (col.IsNull() && slot_desc->is_nullable()) {
            col_ptr->insert_data(nullptr, 0);
            continue;
        } else if (col.IsNull() && !slot_desc->is_nullable()) {
            std::string details = strings::Substitute(INVALID_NULL_VALUE, col_name);
            return Status::RuntimeError(details);
        }
        switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            // sometimes elasticsearch user post some not-string value to Elasticsearch Index.
            // because of reading value from _source, we can not process all json type and then just transfer the value to original string representation
            // this may be a tricky, but we can work around this issue
            std::string val;
            if (pure_doc_value) {
                if (!col[0].IsString()) {
                    val = json_value_to_string(col[0]);
                } else {
                    val = col[0].GetString();
                }
            } else {
                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                if (!col.IsString()) {
                    val = json_value_to_string(col);
                } else {
                    val = col.GetString();
                }
            }
            size_t val_size = val.length();
            col_ptr->insert_data(const_cast<const char*>(val.data()), val_size);
            break;
        }

        case TYPE_TINYINT: {
            insert_int_value<int8_t>(col, type, col_ptr, pure_doc_value, slot_desc->is_nullable());
            break;
        }

        case TYPE_SMALLINT: {
            insert_int_value<int16_t>(col, type, col_ptr, pure_doc_value, slot_desc->is_nullable());
            break;
        }

        case TYPE_INT: {
            insert_int_value<int32>(col, type, col_ptr, pure_doc_value, slot_desc->is_nullable());
            break;
        }

        case TYPE_BIGINT: {
            insert_int_value<int64_t>(col, type, col_ptr, pure_doc_value, slot_desc->is_nullable());
            break;
        }

        case TYPE_LARGEINT: {
            insert_int_value<__int128>(col, type, col_ptr, pure_doc_value,
                                       slot_desc->is_nullable());
            break;
        }

        case TYPE_DOUBLE: {
            insert_float_value<double>(col, type, col_ptr, pure_doc_value,
                                       slot_desc->is_nullable());
            break;
        }

        case TYPE_FLOAT: {
            insert_float_value<float>(col, type, col_ptr, pure_doc_value, slot_desc->is_nullable());
            break;
        }

        case TYPE_BOOLEAN: {
            if (col.IsBool()) {
                int8_t val = col.GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            if (col.IsNumber()) {
                int8_t val = col.GetInt();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            bool is_nested_str = false;
            if (pure_doc_value && col.IsArray() && col[0].IsBool()) {
                int8_t val = col[0].GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            } else if (pure_doc_value && col.IsArray() && col[0].IsString()) {
                is_nested_str = true;
            } else if (pure_doc_value && col.IsArray()) {
                return Status::InternalError(ERROR_INVALID_COL_DATA, "BOOLEAN");
            }

            const rapidjson::Value& str_col = is_nested_str ? col[0] : col;

            const std::string& val = str_col.GetString();
            size_t val_size = str_col.GetStringLength();
            StringParser::ParseResult result;
            bool b = StringParser::string_to_bool(val.c_str(), val_size, &result);
            RETURN_ERROR_IF_PARSING_FAILED(result, str_col, type);
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&b)), 0);
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value data;

            if (col.IsDouble()) {
                data.assign_from_double(col.GetDouble());
            } else {
                std::string val;
                if (pure_doc_value) {
                    if (!col[0].IsString()) {
                        val = json_value_to_string(col[0]);
                    } else {
                        val = col[0].GetString();
                    }
                } else {
                    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                    if (!col.IsString()) {
                        val = json_value_to_string(col);
                    } else {
                        val = col.GetString();
                    }
                }
                data.parse_from_str(val.data(), val.length());
            }
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&data)), 0);
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATEV2:
        case TYPE_DATETIMEV2: {
            // this would happend just only when `enable_docvalue_scan = false`, and field has timestamp format date from _source
            if (col.IsNumber()) {
                // ES process date/datetime field would use millisecond timestamp for index or docvalue
                // processing date type field, if a number is encountered, Doris On ES will force it to be processed according to ms
                // Doris On ES needs to be consistent with ES, so just divided by 1000 because the unit for from_unixtime is seconds
                RETURN_IF_ERROR(fill_date_col(col_ptr, col, type, false));
            } else if (col.IsArray() && pure_doc_value) {
                // this would happened just only when `enable_docvalue_scan = true`
                // ES add default format for all field after ES 6.4, if we not provided format for `date` field ES would impose
                // a standard date-format for date field as `2020-06-16T00:00:00.000Z`
                // At present, we just process this string format date. After some PR were merged into Doris, we would impose `epoch_mills` for
                // date field's docvalue
                if (col[0].IsString()) {
                    RETURN_IF_ERROR(fill_date_col(col_ptr, col[0], type, true));
                    break;
                }
                // ES would return millisecond timestamp for date field, divided by 1000 because the unit for from_unixtime is seconds
                RETURN_IF_ERROR(fill_date_col(col_ptr, col, type, false));
            } else {
                // this would happened just only when `enable_docvalue_scan = false`, and field has string format date from _source
                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
                RETURN_IF_ERROR(fill_date_col(col_ptr, col, type, true));
            }
            break;
        }
        default: {
            DCHECK(false);
            break;
        }
        }
    }

    *line_eof = false;
    return Status::OK();
}

Status ScrollParser::fill_date_col(vectorized::IColumn* col_ptr, const rapidjson::Value& col,
                                   PrimitiveType type, bool is_date_str) {
    const std::string& val = col.GetString();
    size_t val_size = col.GetStringLength();

    if (type == TYPE_DATE || type == TYPE_DATETIME) {
        vectorized::VecDateTimeValue dt_val;
        if ((is_date_str && !dt_val.from_date_str(val.c_str(), val_size)) ||
            (!is_date_str && !dt_val.from_unixtime(col.GetInt64() / 1000, "+08:00"))) {
            RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
        }
        if (type == TYPE_DATE) {
            dt_val.cast_to_date();
        } else {
            dt_val.to_datetime();
        }

        auto date_packed_int = binary_cast<doris::vectorized::VecDateTimeValue, int64_t>(
                *reinterpret_cast<vectorized::VecDateTimeValue*>(&dt_val));
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
        return Status::OK();
    } else if (type == TYPE_DATEV2) {
        vectorized::DateV2Value<doris::vectorized::DateV2ValueType> dt_val;
        if ((is_date_str && !dt_val.from_date_str(val.c_str(), val_size)) ||
            (!is_date_str && !dt_val.from_unixtime(col.GetInt64() / 1000, "+08:00"))) {
            RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
        }
        auto date_packed_int = binary_cast<
                doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>, uint32_t>(
                *reinterpret_cast<vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(
                        &dt_val));
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
        return Status::OK();

    } else if (type == TYPE_DATETIMEV2) {
        vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> dt_val;
        if ((is_date_str && !dt_val.from_date_str(val.c_str(), val_size)) ||
            (!is_date_str && !dt_val.from_unixtime(col.GetInt64() / 1000, "+08:00"))) {
            RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
        }
        auto date_packed_int = binary_cast<
                vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>, uint64_t>(
                *reinterpret_cast<vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(
                        &dt_val));
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&date_packed_int)), 0);
        return Status::OK();

    } else {
        return Status::InternalError("Unsupported datetime type.");
    }
}

} // namespace doris
