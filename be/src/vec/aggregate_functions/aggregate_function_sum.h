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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <memory>
#include <type_traits>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_map.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_map.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <typename T>
class ColumnDecimal;
template <typename T>
class DataTypeNumber;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionSumData {
    T sum {};

    void add(T value) {
#ifdef __clang__
#pragma clang fp reassociate(on)
#endif
        sum += value;
    }

    void merge(const AggregateFunctionSumData& rhs) { sum += rhs.sum; }

    void write(BufferWritable& buf) const { write_binary(sum, buf); }

    void read(BufferReadable& buf) { read_binary(sum, buf); }

    T get() const { return sum; }
};

struct AggregateFunctionMapSumData {
    using Map = phmap::flat_hash_map<StringRef, int64_t>;
    Map _map {};

    AggregateFunctionMapSumData() = default;
    AggregateFunctionMapSumData(const DataTypes& argument_types) {}

    void add(StringRef key, int64_t value) {
        DCHECK(key.data != nullptr);
        if (_map.find(key) != _map.end()) {
            _map[key] = _map[key] + value;
        } else {
            _map.emplace(key, value);
        }
    }

    void add(const Field& key_, const Field& value) {
        DCHECK(!key_.is_null());
        auto key_array = vectorized::get<Array>(key_);
        auto value_array = vectorized::get<Array>(value);

        const auto count = key_array.size();
        DCHECK_EQ(count, value_array.size());

        for (size_t i = 0; i != count; ++i) {
            StringRef key;
            auto& string = key_array[i].get<String>();
            key.data = string.data();
            key.size = string.size();

            if (_map.find(key) != _map.end()) {
                _map[key] = _map[key] + value_array[i].get<int64_t>();
            } else {
                key.data = _arena.insert(key.data, key.size);
                _map.emplace(key, value_array[i].get<int64_t>());
            }
        }
    }

    void merge(const AggregateFunctionMapSumData& rhs) {
        const size_t num_rows = rhs._map.size();
        if (num_rows <= 0) {
            return;
        }

        for (const auto& pair : rhs._map) {
            add(pair.first, pair.second);
        }
    }

    void write(BufferWritable& buf) const {
        const size_t size = _map.size();
        write_binary(size, buf);
        for (const auto& pair : _map) {
            write_binary(pair.first, buf);
            write_binary(pair.second, buf);
        }
    }

    void read(BufferReadable& buf) {
        size_t size = 0;
        read_binary(size, buf);
        StringRef key;
        int64_t value;
        for (size_t i = 0; i < size; i++) {
            read_binary(key, buf);
            read_binary(value, buf);
            add(key, value);
        }
    }

    void reset() {
        _map = {};
    }

    Map get() const { return _map; }

private:
    Arena _arena;
};

/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>> {
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>,
                                              DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult =
            std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String get_name() const override { return "sum"; }

    AggregateFunctionSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(
                      argument_types_),
              scale(get_decimal_scale(*argument_types_[0])) {}

    DataTypePtr get_return_type() const override {
        if constexpr (IsDecimalNumber<T>) {
            return std::make_shared<ResultDataType>(ResultDataType::max_precision(), scale);
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        const auto& column = assert_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(TResult(column.get_data()[row_num]));
    }

    void reset(AggregateDataPtr place) const override { this->data(place).sum = {}; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(this->data(place).get());
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = col.get_data().data();
        memcpy(places, data, sizeof(Data) * num_rows);
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            *reinterpret_cast<Data*>(&data[sizeof(Data) * i]) =
                    *reinterpret_cast<Data*>(places[i] + offset);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        auto& src = assert_cast<const ColVecType&>(*columns[0]);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        col.resize(num_rows);
        auto* src_data = src.get_data().data();
        auto* dst_data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto& state = *reinterpret_cast<Data*>(&dst_data[sizeof(Data) * i]);
            state.sum = TResult(src_data[i]);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t num_rows = column.size();
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).sum += data[i].sum;
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = reinterpret_cast<const Data*>(col.get_data().data());
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).sum += data[i].sum;
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena* arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena* arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        DCHECK(col.item_size() == sizeof(Data))
                << "size is not equal: " << col.item_size() << " " << sizeof(Data);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        (reinterpret_cast<Data*>(col.get_data().data()) + old_size)->sum = this->data(place).sum;
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(Data));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }

private:
    UInt32 scale;
};

/// Counts the sum of the numbers.
template <typename Data>
class AggregateFunctionMapSum final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionMapSum<Data>> {
public:

    String get_name() const override { return "map_sum"; }

    AggregateFunctionMapSum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionMapSum<Data>>(
                      argument_types_) {}

    DataTypePtr get_return_type() const override {
        /// keys and values column of `ColumnMap` are always nullable.
        return std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                             make_nullable(std::make_shared<DataTypeInt64>()));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        auto& col = assert_cast<const ColumnMap&>(*columns[0]);
        auto map = doris::vectorized::get<Map>(col[row_num]);
        this->data(place).add(map[0], map[1]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& dst = assert_cast<ColumnMap&>(to);
        size_t num_rows = this->data(place).get().size();
        auto& offsets = dst.get_offsets();
        auto& dst_key_column = assert_cast<ColumnNullable&>(dst.get_keys());
        dst_key_column.get_null_map_data().resize_fill(dst_key_column.get_null_map_data().size() +
                                                       num_rows, 0);

        for (const auto& pair : this->data(place).get()) {
            dst_key_column.get_nested_column().insert_data(pair.first.data, pair.first.size);
            dst.get_values().insert(pair.second);
        }

        if (offsets.empty()) {
            offsets.push_back(num_rows);
        } else {
            offsets.push_back(offsets.back() + num_rows);
        }
    }
};

template <typename T, bool level_up>
struct SumSimple {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<level_up, DisposeDecimal<T, NearestFieldType<T>>, T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimple = typename SumSimple<T, true>::Function;

struct MapSumSimple {
    using Function = AggregateFunctionMapSum<AggregateFunctionMapSumData>;
};

using AggregateFunctionMapSumSimple = typename MapSumSimple::Function;

const static std::string DECIMAL256_SUFFIX {"_decimal256"};
template <typename T, bool level_up>
struct SumSimpleDecimal256 {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<level_up, DisposeDecimal256<T, NearestFieldType<T>>, T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimpleDecimal256 = typename SumSimpleDecimal256<T, true>::Function;

// do not level up return type for agg reader
template <typename T>
using AggregateFunctionSumSimpleReader = typename SumSimple<T, false>::Function;

} // namespace doris::vectorized
