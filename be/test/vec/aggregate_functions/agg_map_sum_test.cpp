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

#include <fmt/format.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_sum.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
// declare function
void register_aggregate_function_map_sum(AggregateFunctionSimpleFactory& factory);

class AggMapSumTest : public testing::Test {};

TEST_F(AggMapSumTest, map_sum_test) {
    // Prepare test data.
    std::cout << "==== map<string,int64> === " << std::endl;
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
    Array k1, k2, v1, v2;
    k1.push_back("vat1");
    k1.push_back("vat2");
    v1.push_back(100);
    v1.push_back(20);
    k2.push_back("vat1");
    k2.push_back("vat3");
    v2.push_back(200);
    v2.push_back(50);
    Map m1, m2;
    m1.push_back(k1);
    m1.push_back(v1);
    m2.push_back(k2);
    m2.push_back(v2);
    MutableColumnPtr map_column = m->create_column();
    map_column->reserve(2);
    map_column->insert(m1);
    map_column->insert(m2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_map_sum(factory);

    auto agg_function = factory.get("map_sum", {m});
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* columns[1] = {map_column.get()};
    for (int j = 0; j < 2; j++) {
        agg_function->add(place, columns, j, nullptr);
    }

    // Check result.
    MutableColumnPtr result_column = m->create_column();
    agg_function->insert_result_into(place, *result_column);
    Map res = doris::vectorized::get<Map>((*result_column)[0]);

    auto key_array = vectorized::get<Array>(res[0]);
    auto value_array = vectorized::get<Array>(res[1]);
    size_t res_size = key_array.size();

    EXPECT_EQ(3, res_size);
    phmap::flat_hash_map<StringRef, int64_t> res_map;
    for (size_t i = 0; i < res_size; i++) {
        res_map.emplace(key_array[i].get<String>(),value_array[i].get<int64_t>());
    }
    EXPECT_EQ(300, res_map[StringRef("vat1")]);
    EXPECT_EQ(20, res_map[StringRef("vat2")]);
    EXPECT_EQ(50, res_map[StringRef("vat3")]);
    agg_function->destroy(place);
}

} // namespace doris::vectorized
