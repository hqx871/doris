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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;

/**
 * ScalarFunction 'if'. This class is generated by GenerateFunction.
 */
public class If extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature, ComputePrecision {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(BooleanType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE),
            FunctionSignature.ret(TinyIntType.INSTANCE)
                    .args(BooleanType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(SmallIntType.INSTANCE)
                    .args(BooleanType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE)
                    .args(BooleanType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE)
                    .args(BooleanType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(LargeIntType.INSTANCE)
                    .args(BooleanType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE),
            FunctionSignature.ret(FloatType.INSTANCE)
                    .args(BooleanType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE)
                    .args(BooleanType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE),
            FunctionSignature.ret(DateTimeType.INSTANCE)
                    .args(BooleanType.INSTANCE, DateTimeType.INSTANCE, DateTimeType.INSTANCE),
            FunctionSignature.ret(DateType.INSTANCE).args(BooleanType.INSTANCE, DateType.INSTANCE, DateType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(DateV2Type.INSTANCE)
                    .args(BooleanType.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE),
            FunctionSignature.ret(DecimalV2Type.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(DecimalV3Type.DEFAULT_DECIMAL32)
                    .args(BooleanType.INSTANCE, DecimalV3Type.DEFAULT_DECIMAL32, DecimalV3Type.DEFAULT_DECIMAL32),
            FunctionSignature.ret(DecimalV3Type.DEFAULT_DECIMAL64)
                    .args(BooleanType.INSTANCE, DecimalV3Type.DEFAULT_DECIMAL64, DecimalV3Type.DEFAULT_DECIMAL64),
            FunctionSignature.ret(DecimalV3Type.DEFAULT_DECIMAL128)
                    .args(BooleanType.INSTANCE, DecimalV3Type.DEFAULT_DECIMAL128, DecimalV3Type.DEFAULT_DECIMAL128),
            FunctionSignature.ret(BitmapType.INSTANCE)
                    .args(BooleanType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE),
            FunctionSignature.ret(HllType.INSTANCE).args(BooleanType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(BooleanType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(BooleanType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(BooleanType.INSTANCE), ArrayType.of(BooleanType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(TinyIntType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(TinyIntType.INSTANCE), ArrayType.of(TinyIntType.INSTANCE)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(SmallIntType.INSTANCE),
                            ArrayType.of(SmallIntType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(IntegerType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(IntegerType.INSTANCE), ArrayType.of(IntegerType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(BigIntType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(BigIntType.INSTANCE), ArrayType.of(BigIntType.INSTANCE)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(LargeIntType.INSTANCE),
                            ArrayType.of(LargeIntType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(FloatType.INSTANCE), ArrayType.of(FloatType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(DoubleType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(DoubleType.INSTANCE), ArrayType.of(DoubleType.INSTANCE)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(DateTimeType.INSTANCE),
                            ArrayType.of(DateTimeType.INSTANCE)),
            FunctionSignature.ret(ArrayType.of(DateType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(DateType.INSTANCE), ArrayType.of(DateType.INSTANCE)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT),
                            ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT)),
            FunctionSignature.ret(ArrayType.of(DateV2Type.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(DateV2Type.INSTANCE), ArrayType.of(DateV2Type.INSTANCE)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(DecimalV2Type.SYSTEM_DEFAULT),
                            ArrayType.of(DecimalV2Type.SYSTEM_DEFAULT)),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(BooleanType.INSTANCE,
                            ArrayType.of(VarcharType.SYSTEM_DEFAULT),
                            ArrayType.of(VarcharType.SYSTEM_DEFAULT)),
            FunctionSignature.ret(ArrayType.of(StringType.INSTANCE))
                    .args(BooleanType.INSTANCE, ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE))
    );

    private final Supplier<DataType> widerType = Suppliers.memoize(() -> {
        Type assignmentCompatibleType = ScalarType.getAssignmentCompatibleType(
                getArgumentType(1).toCatalogDataType(),
                getArgumentType(2).toCatalogDataType(),
                true);
        return DataType.fromCatalogType(assignmentCompatibleType);
    });

    /**
     * constructor with 3 arguments.
     */
    public If(Expression arg0, Expression arg1, Expression arg2) {
        super("if", arg0, arg1, arg2);
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        if (getArgumentType(1).isDecimalV3Type() && getArgumentType(2).isDecimalV3Type()) {
            DecimalV3Type trueType = (DecimalV3Type) getArgumentType(1);
            DecimalV3Type falseType = (DecimalV3Type) getArgumentType(2);
            return signature.withReturnType(DecimalV3Type.createDecimalV3Type(
                    Math.max(trueType.getPrecision(), falseType.getPrecision()),
                    Math.max(trueType.getScale(), falseType.getPrecision())
            ));
        } else if (getArgumentType(1).isDateTimeV2Type() && getArgumentType(2).isDateTimeV2Type()) {
            DateTimeV2Type trueType = (DateTimeV2Type) getArgumentType(1);
            DateTimeV2Type falseType = (DateTimeV2Type) getArgumentType(2);
            return signature.withReturnType(trueType.getScale() > falseType.getScale() ? trueType : falseType);
        } else {
            return signature;
        }
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        DataType widerType = this.widerType.get();
        List<AbstractDataType> newArgumentsTypes = new ImmutableList.Builder<AbstractDataType>()
                .add(signature.argumentsTypes.get(0))
                .add(widerType)
                .add(widerType)
                .build();
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgumentsTypes)
                .withReturnType(widerType);
        return super.computeSignature(signature);
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        for (int i = 1; i < arity(); i++) {
            if (child(i).nullable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * withChildren.
     */
    @Override
    public If withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new If(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIf(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
