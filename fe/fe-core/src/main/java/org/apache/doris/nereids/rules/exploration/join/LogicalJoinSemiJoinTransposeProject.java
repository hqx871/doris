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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.ArrayList;

/**
 * LogicalJoin(SemiJoin(A, B), C) -> SemiJoin(LogicalJoin(A, C), B)
 */
public class LogicalJoinSemiJoinTransposeProject extends OneExplorationRuleFactory {

    public static final LogicalJoinSemiJoinTransposeProject INSTANCE = new LogicalJoinSemiJoinTransposeProject();

    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(topJoin -> (topJoin.left().child().getJoinType().isLeftSemiOrAntiJoin()
                        && (topJoin.getJoinType().isInnerJoin()
                        || topJoin.getJoinType().isLeftOuterJoin())))
                .whenNot(topJoin -> topJoin.hasJoinHint() || topJoin.left().child().hasJoinHint())
                .whenNot(LogicalJoin::isMarkJoin)
                .when(join -> JoinReorderUtils.isAllSlotProject(join.left()))
                .then(topJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    // Discard this project, because it is useless.
                    Plan newBottomJoin = topJoin.withChildren(a, c);
                    Plan newTopJoin = bottomJoin.withChildren(newBottomJoin, b);
                    return JoinReorderUtils.projectOrSelfInOrder(new ArrayList<>(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_PROJECT);
    }
}
