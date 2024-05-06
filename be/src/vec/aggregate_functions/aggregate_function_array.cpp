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

#include "vec/aggregate_functions/aggregate_function_array.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

void register_aggregate_function_array(AggregateFunctionSimpleFactory& factory,
                                                     const std::string& name,
                                                     const std::string& nested_name) {
    auto creator = [&factory, nested_name](const std::string& name,
                                            const DataTypes& argument_types,
                                            const Array& parameters,
                                            const bool result_is_nullable) -> AggregateFunctionPtr {
        DataTypes nested_argument_types;
        nested_argument_types.reserve(argument_types.size());
        for (const auto& type : argument_types) {
            if (const DataTypeArray* array = typeid_cast<const DataTypeArray*>(type.get())) {
                nested_argument_types.push_back(array->get_nested_type());
            } else {
                LOG(WARNING) << "All arguments for aggregate function " << name << " must be arrays";
                return nullptr;
            }
        }
        AggregateFunctionPtr nested_func =
                factory.get(nested_name, nested_argument_types, parameters, result_is_nullable);
        if (nested_func == nullptr) {
            LOG(WARNING) << "Cannot find nested aggregate function " << nested_name;
            return nullptr;
        }
        return AggregateFunctionPtr(
                new AggregateFunctionArray(name, nested_func, argument_types, parameters));
    };
    factory.register_function(name, creator);
}

} // namespace doris::vectorized
