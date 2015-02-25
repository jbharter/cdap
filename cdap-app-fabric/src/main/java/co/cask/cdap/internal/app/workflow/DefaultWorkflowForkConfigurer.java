/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.clearspring.analytics.util.Lists;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Default implementation of the {@link WorkflowForkConfigurer}
 * @param <T>
 */
public class DefaultWorkflowForkConfigurer<T> implements WorkflowForkConfigurer<T> {

  private final T parentForkConfigurer;
  private final WorkflowConfigurer workflowConfigurer;

  private final List<List<WorkflowNode>> branches = Lists.newArrayList();
  private List<WorkflowNode> currentBranch;

  public DefaultWorkflowForkConfigurer(WorkflowConfigurer workflowConfigurer,
                                       @Nullable T parentForkConfigurer) {
    this.parentForkConfigurer = parentForkConfigurer;
    this.workflowConfigurer = workflowConfigurer;
    currentBranch = Lists.newArrayList();
  }

  @Override
  public WorkflowForkConfigurer<T> addMapReduce(String mapReduce) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).createWorkflowActionNode
      (mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addSpark(String spark) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).createWorkflowActionNode
      (spark, SchedulableProgramType.CUSTOM_ACTION));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addAction(WorkflowAction action) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).createWorkflowCustomActionNode(action));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowForkConfigurer<WorkflowForkConfigurer<T>> fork() {
    return new DefaultWorkflowForkConfigurer<WorkflowForkConfigurer<T>>
        (workflowConfigurer, (WorkflowForkConfigurer<T>) this);
  }

  @Override
  public WorkflowForkConfigurer<T> also() {
    branches.add(currentBranch);
    currentBranch = Lists.newArrayList();
    return this;
  }

  public void addWorkflowForkNode(List<List<WorkflowNode>> branch) {
    currentBranch.add(new WorkflowForkNode(null, branch));
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public T join() {
    branches.add(currentBranch);
    if (parentForkConfigurer == null) {
      ((DefaultWorkflowConfigurer) workflowConfigurer).addWorkflowForkNode(branches);
    } else {
      ((DefaultWorkflowForkConfigurer<WorkflowForkConfigurer<T>>) parentForkConfigurer).addWorkflowForkNode(branches);
    }
    return parentForkConfigurer;
  }
}
