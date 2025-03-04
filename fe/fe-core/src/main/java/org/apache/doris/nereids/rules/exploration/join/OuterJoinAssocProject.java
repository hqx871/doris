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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OuterJoinAssocProject.
 */
public class OuterJoinAssocProject extends OneExplorationRuleFactory {
    /*
     *        topJoin        newTopJoin
     *        /     \         /     \
     *   bottomJoin  C  ->   A   newBottomJoin
     *    /    \                     /    \
     *   A      B                   B      C
     */
    public static final OuterJoinAssocProject INSTANCE = new OuterJoinAssocProject();

    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(join -> OuterJoinAssoc.VALID_TYPE_PAIR_SET.contains(
                        Pair.of(join.left().child().getJoinType(), join.getJoinType())))
                .when(topJoin -> OuterJoinLAsscom.checkReorder(topJoin, topJoin.left().child()))
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .when(join -> OuterJoinAssoc.checkCondition(join, join.left().child().left().getOutputSet()))
                .then(topJoin -> {
                    /* ********** init ********** */
                    List<NamedExpression> projects = topJoin.left().getProjects();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    /* ********** Split projects ********** */
                    Map<Boolean, List<NamedExpression>> map = JoinReorderUtils.splitProjection(projects, a);
                    List<NamedExpression> aProjects = map.get(true);
                    List<NamedExpression> bProjects = map.get(false);
                    if (bProjects.isEmpty()) {
                        return null;
                    }
                    Set<ExprId> aProjectsExprIds = aProjects.stream().map(NamedExpression::getExprId)
                            .collect(Collectors.toSet());

                    // topJoin condition can't contain aProject. just can (B C)
                    if (Stream.concat(topJoin.getHashJoinConjuncts().stream(), topJoin.getOtherJoinConjuncts().stream())
                            .anyMatch(expr -> Utils.isIntersecting(expr.getInputSlotExprIds(), aProjectsExprIds))) {
                        return null;
                    }

                    // topJoin condition -> newBottomJoin condition, bottomJoin condition -> newTopJoin condition
                    JoinReorderHelper helper = new JoinReorderHelper(bottomJoin.getHashJoinConjuncts(),
                            bottomJoin.getOtherJoinConjuncts(), topJoin.getHashJoinConjuncts(),
                            topJoin.getOtherJoinConjuncts(), projects, aProjects, bProjects);

                    // Add all slots used by OnCondition when projects not empty.
                    helper.addSlotsUsedByOn(JoinReorderUtils.combineProjectAndChildExprId(a, helper.newLeftProjects),
                            Collections.EMPTY_SET);

                    bProjects.addAll(OuterJoinLAsscomProject.forceToNullable(c.getOutputSet()));
                    /* ********** new Plan ********** */
                    LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                            helper.newBottomHashConjuncts, helper.newBottomOtherConjuncts, JoinHint.NONE,
                            b, c, bottomJoin.getJoinReorderContext());

                    Plan left = JoinReorderUtils.projectOrSelf(aProjects, a);
                    Plan right = JoinReorderUtils.projectOrSelf(bProjects, newBottomJoin);

                    LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                            helper.newTopHashConjuncts, helper.newTopOtherConjuncts, JoinHint.NONE,
                            left, right, topJoin.getJoinReorderContext());
                    OuterJoinAssoc.setReorderContext(newTopJoin, newBottomJoin);

                    return JoinReorderUtils.projectOrSelf(new ArrayList<>(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_ASSOC_PROJECT);
    }
}
