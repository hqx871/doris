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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHash.cpp
// and modified by Doris
#include "../data_types/data_type_string.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
    class FunctionToString : public IFunction {
    public:
        static constexpr auto name = "to_string";

        static FunctionPtr create() { return std::make_shared<FunctionToString>(); }


        String get_name() const override { return name; }

        size_t get_number_of_arguments() const override { return 1; }

        DataTypePtr get_return_type_impl(const DataTypes &arguments) const override {
            return std::make_shared<DataTypeString>();
        }

        Status execute_impl(FunctionContext *context, Block &block, const ColumnNumbers &arguments,
                            size_t result, size_t input_rows_count) const override {
            auto data_type_to = std::make_shared<DataTypeString>();

            const auto &col_with_type_and_name = block.get_by_position(arguments[0]);
            const IDataType &type = *col_with_type_and_name.type;
            const IColumn &col_from = *col_with_type_and_name.column;

            auto column_string = ColumnString::create();
            JsonbWriter writer;

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(col_from.size());
            ColumnUInt8::Container *vec_null_map_to = &col_null_map_to->get_data();
            DataTypeSerDe::FormatOptions format_options;
            format_options.converted_from_string = true;
            DataTypeSerDeSPtr from_serde = type.get_serde();
            DataTypeSerDeSPtr to_serde = data_type_to->get_serde();
            auto col_to = data_type_to->create_column();

            auto tmp_col = ColumnString::create();
            vectorized::DataTypeSerDe::FormatOptions options;
            options.escape_char = '\\';
            for (size_t i = 0; i < input_rows_count; i++) {
                // convert to string
                tmp_col->clear();
                VectorBufferWriter write_buffer(*tmp_col.get());
                Status st =
                        from_serde->serialize_column_to_json(col_from, i, i + 1, write_buffer, options);
                // if serialized failed, will return null
                (*vec_null_map_to)[i] = !st.ok();
                if (!st.ok()) {
                    col_to->insert_default();
                    continue;
                }
                write_buffer.commit();
                writer.reset();
                auto str_ref = tmp_col->get_data_at(0);
                Slice data((char *) (str_ref.data), str_ref.size);
                // first try to parse string
                st = to_serde->deserialize_one_cell_from_json(*col_to, data, format_options);
                // if parsing failed, will return null
                (*vec_null_map_to)[i] = !st.ok();
                if (!st.ok()) {
                    col_to->insert_default();
                }
            }

            block.replace_by_position(
                result, ColumnNullable::create(std::move(col_to), std::move(col_null_map_to)));
            return Status::OK();
        }
    };

    void register_function_to_string(SimpleFunctionFactory &factory) {
        factory.register_function<FunctionToString>();
    }
} // namespace doris::vectorized
