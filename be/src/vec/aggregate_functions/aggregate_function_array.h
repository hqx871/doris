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

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `aggArray(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
class AggregateFunctionArray final : public IAggregateFunctionHelper<AggregateFunctionArray> {
private:
    const String name;
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionArray(const String& name_, AggregateFunctionPtr nested_,
                           const DataTypes& arguments, const Array& params_)
            : IAggregateFunctionHelper<AggregateFunctionArray>(arguments, params_),
              name(name_),
              nested_func(nested_),
              num_arguments(arguments.size()) {}

    String get_name() const override { return name; }

    DataTypePtr get_return_type() const override { return nested_func->get_return_type(); }

    void create(AggregateDataPtr __restrict place) const override { nested_func->create(place); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        nested_func->destroy(place);
    }

    bool has_trivial_destructor() const override { return nested_func->has_trivial_destructor(); }

    size_t size_of_data() const override { return nested_func->size_of_data(); }

    /// NOTE: Currently not used (structures with aggregation state are put without alignment).
    size_t align_of_data() const override { return nested_func->align_of_data(); }

    void deserialize_and_merge(AggregateDataPtr __restrict place, BufferReadable& buf,
                               Arena* arena) const override {
        nested_func->deserialize_and_merge(place, buf, arena);
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        nested_func->deserialize_and_merge_from_column(place, column, arena);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        const IColumn* nested[num_arguments];

        for (size_t i = 0; i < num_arguments; ++i)
            nested[i] = &assert_cast<const ColumnArray&>(*columns[i]).get_data();

        const ColumnArray& first_array_column = assert_cast<const ColumnArray&>(*columns[0]);
        const auto& offsets = first_array_column.get_offsets();

        size_t begin = offsets[row_num - 1];
        size_t end = offsets[row_num];

        for (size_t i = begin; i < end; ++i) nested_func->add(place, nested, i, arena);
    }

    void reset(AggregateDataPtr place) const override { nested_func->reset(place); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        nested_func->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        nested_func->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        nested_func->deserialize(place, buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        nested_func->insert_result_into(place, to);
    }

    bool allocates_memory_in_arena() const override {
        return nested_func->allocates_memory_in_arena();
    }
};

void register_aggregate_function_array(AggregateFunctionSimpleFactory& factory,
                                       const std::string& name, const std::string& nested_name);
} // namespace doris::vectorized
