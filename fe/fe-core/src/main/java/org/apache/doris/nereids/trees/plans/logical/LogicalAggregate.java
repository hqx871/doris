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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Aggregate plan.
 * <p>
 * For example SQL:
 * <p>
 * select a, sum(b), c from table group by a, c;
 * <p>
 * groupByExpressions: Column field after group by. eg: a, c;
 * outputExpressions: Column field after select. eg: a, sum(b), c;
 * <p>
 * Each agg node only contains the select statement field of the same layer,
 * and other agg nodes in the subquery contain.
 * Note: In general, the output of agg is a subset of the group by column plus aggregate column.
 * In special cases. this relationship does not hold. for example, select k1+1, sum(v1) from table group by k1.
 */
public class LogicalAggregate<CHILD_TYPE extends Plan>
        extends LogicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE>, OutputSavePoint {

    private final boolean normalized;
    private final List<Expression> groupByExpressions;
    private final List<NamedExpression> outputExpressions;

    // When there are grouping sets/rollup/cube, LogicalAgg is generated by LogicalRepeat.
    private final Optional<LogicalRepeat> sourceRepeat;

    private final boolean ordinalIsResolved;

    /**
     * Desc: Constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions,
                false, Optional.empty(), child);
    }

    public LogicalAggregate(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions, boolean ordinalIsResolved, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, false, ordinalIsResolved, Optional.empty(),
                Optional.empty(), Optional.empty(), child);
    }

    /**
     * Desc: Constructor for LogicalAggregate.
     * Generated from LogicalRepeat.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<LogicalRepeat> sourceRepeat,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, false, sourceRepeat, child);
    }

    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            boolean normalized,
            Optional<LogicalRepeat> sourceRepeat,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, normalized, false, sourceRepeat,
                Optional.empty(), Optional.empty(), child);
    }

    /**
     * Whole parameters constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            boolean normalized,
            boolean ordinalIsResolved,
            Optional<LogicalRepeat> sourceRepeat,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.normalized = normalized;
        this.ordinalIsResolved = ordinalIsResolved;
        this.sourceRepeat = Objects.requireNonNull(sourceRepeat, "sourceRepeat cannot be null");
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public Optional<LogicalRepeat> getSourceRepeat() {
        return sourceRepeat;
    }

    public boolean hasRepeat() {
        return sourceRepeat.isPresent();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalAggregate[" + id.asInt() + "]",
                "groupByExpr", groupByExpressions,
                "outputExpr", outputExpressions,
                "hasRepeat", sourceRepeat.isPresent()
        );
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .build();
    }

    public boolean isNormalized() {
        return normalized;
    }

    public boolean isOrdinalIsResolved() {
        return ordinalIsResolved;
    }

    /**
     * Determine the equality with another plan
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalAggregate that = (LogicalAggregate) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && normalized == that.normalized
                && ordinalIsResolved == that.ordinalIsResolved
                && Objects.equals(sourceRepeat, that.sourceRepeat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, normalized, ordinalIsResolved, sourceRepeat);
    }

    @Override
    public LogicalAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalAggregate<>(groupByExpressions, outputExpressions,
                normalized, ordinalIsResolved, sourceRepeat, Optional.empty(), Optional.empty(), children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalAggregate<>(groupByExpressions, outputExpressions,
                normalized, ordinalIsResolved, sourceRepeat, groupExpression, Optional.of(getLogicalProperties()),
                children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalAggregate<>(groupByExpressions, outputExpressions,
                normalized, ordinalIsResolved, sourceRepeat,
                Optional.empty(), logicalProperties, children.get(0));
    }

    public LogicalAggregate<Plan> withGroupByAndOutput(List<Expression> groupByExprList,
            List<NamedExpression> outputExpressionList) {
        return new LogicalAggregate<>(groupByExprList, outputExpressionList, normalized, ordinalIsResolved,
                sourceRepeat, Optional.empty(), Optional.empty(), child());
    }

    @Override
    public LogicalAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new LogicalAggregate<>(groupByExpressions, newOutput, normalized, ordinalIsResolved,
                sourceRepeat, Optional.empty(), Optional.empty(), child());
    }

    public LogicalAggregate<Plan> withNormalized(List<Expression> normalizedGroupBy,
            List<NamedExpression> normalizedOutput, Plan normalizedChild) {
        return new LogicalAggregate<>(normalizedGroupBy, normalizedOutput, true, ordinalIsResolved,
                sourceRepeat, Optional.empty(),
                Optional.empty(), normalizedChild);
    }
}
