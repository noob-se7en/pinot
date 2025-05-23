/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Priority queues of scheduler groups that determines query priority based on tokens
 *
 * This is a multi-level query scheduling queue with each sublevel maintaining a waitlist of
 * queries for the group. The priority between groups is provided by specific SchedulerGroup
 * implementation. If two groups have the same priority then the group with lower
 * resource utilization is selected first. Oldest query from the winning SchedulerGroup
 * is selected for execution.
 */
public class MultiLevelPriorityQueue implements SchedulerPriorityQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiLevelPriorityQueue.class);
  public static final String QUERY_DEADLINE_SECONDS_KEY = "query_deadline_seconds";
  public static final String MAX_PENDING_PER_GROUP_KEY = "max_pending_per_group";
  public static final String QUEUE_WAKEUP_MICROS = "queue_wakeup_micros";

  private static final int DEFAULT_WAKEUP_MICROS = 1000;

  private static int _wakeUpTimeMicros = DEFAULT_WAKEUP_MICROS;
  private final int _maxPendingPerGroup;

  private final Map<String, SchedulerGroup> _schedulerGroups = new HashMap<>();
  private final Lock _queueLock = new ReentrantLock();
  private final Condition _queryReaderCondition = _queueLock.newCondition();
  private final ResourceManager _resourceManager;
  private final SchedulerGroupMapper _groupSelector;
  private final int _queryDeadlineMillis;
  private final SchedulerGroupFactory _groupFactory;
  private final PinotConfiguration _config;

  public MultiLevelPriorityQueue(PinotConfiguration config, ResourceManager resourceManager,
      SchedulerGroupFactory groupFactory, SchedulerGroupMapper groupMapper) {
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(resourceManager);
    Preconditions.checkNotNull(groupFactory);
    Preconditions.checkNotNull(groupMapper);

    // max available tokens per millisecond equals number of threads (total execution capacity)
    // we are over provisioning tokens here because its better to keep pipe full rather than empty
    _queryDeadlineMillis = config.getProperty(QUERY_DEADLINE_SECONDS_KEY, 30) * 1000;
    _wakeUpTimeMicros = config.getProperty(QUEUE_WAKEUP_MICROS, DEFAULT_WAKEUP_MICROS);
    _maxPendingPerGroup = config.getProperty(MAX_PENDING_PER_GROUP_KEY, 10);
    _config = config;
    _resourceManager = resourceManager;
    _groupFactory = groupFactory;
    _groupSelector = groupMapper;
  }

  @Override
  public void put(SchedulerQueryContext query)
      throws OutOfCapacityException {
    Preconditions.checkNotNull(query);
    _queueLock.lock();
    String groupName = _groupSelector.getSchedulerGroupName(query);
    try {
      SchedulerGroup groupContext = getOrCreateGroupContext(groupName);
      checkGroupHasCapacity(groupContext);
      query.setSchedulerGroupContext(groupContext);
      groupContext.addLast(query);
      _queryReaderCondition.signal();
    } finally {
      _queueLock.unlock();
    }
  }

  /**
   * Blocking call to read the next query in order of priority
   * @return
   */
  @Nullable
  @Override
  public SchedulerQueryContext take() {
    _queueLock.lock();
    try {
      while (true) {
        SchedulerQueryContext schedulerQueryContext;
        while ((schedulerQueryContext = takeNextInternal()) == null) {
          try {
            _queryReaderCondition.await(_wakeUpTimeMicros, TimeUnit.MICROSECONDS);
          } catch (InterruptedException e) {
            return null;
          }
        }
        return schedulerQueryContext;
      }
    } finally {
      _queueLock.unlock();
    }
  }

  @Override
  public List<SchedulerQueryContext> drain() {
    List<SchedulerQueryContext> pending = new ArrayList<>();
    _queueLock.lock();
    try {
      for (Map.Entry<String, SchedulerGroup> groupEntry : _schedulerGroups.entrySet()) {
        SchedulerGroup group = groupEntry.getValue();
        while (!group.isEmpty()) {
          pending.add(group.removeFirst());
        }
      }
    } finally {
      _queueLock.unlock();
    }
    return pending;
  }

  private SchedulerQueryContext takeNextInternal() {
    SchedulerGroup currentWinnerGroup = null;
    long startTime = System.nanoTime();
    StringBuilder sb = new StringBuilder("SchedulerInfo:");
    long deadlineEpochMillis = currentTimeMillis() - _queryDeadlineMillis;
    for (Map.Entry<String, SchedulerGroup> groupInfoEntry : _schedulerGroups.entrySet()) {
      SchedulerGroup group = groupInfoEntry.getValue();
      if (LOGGER.isDebugEnabled()) {
        sb.append(group.toString());
      }
      group.trimExpired(deadlineEpochMillis);
      if (group.isEmpty() || !_resourceManager.canSchedule(group)) {
        continue;
      }

      if (currentWinnerGroup == null) {
        currentWinnerGroup = group;
        continue;
      }

      // Preconditions:
      // a. currentGroupResources <= hardLimit
      // b. selectedGroupResources <= hardLimit
      // We prefer group with higher priority but with resource limits.
      // If current group priority are greater than currently winning priority then we choose current
      // group over currentWinnerGroup if
      // a. current group is using less than softLimit resources
      // b. if softLimit < currentGroupResources <= hardLimit then
      //     i. choose group if softLimit <= currentWinnerGroup <= hardLimit
      //     ii. continue with currentWinnerGroup otherwise
      int comparison = group.compareTo(currentWinnerGroup);
      if (comparison < 0) {
        if (currentWinnerGroup.totalReservedThreads() > _resourceManager.getTableThreadsSoftLimit()
            && group.totalReservedThreads() < _resourceManager.getTableThreadsSoftLimit()) {
          currentWinnerGroup = group;
        }
        continue;
      }
      if (comparison >= 0) {
        if (group.totalReservedThreads() < _resourceManager.getTableThreadsSoftLimit()
            || group.totalReservedThreads() < currentWinnerGroup.totalReservedThreads()) {
          currentWinnerGroup = group;
        }
      }
    }

    SchedulerQueryContext query = null;
    if (currentWinnerGroup != null) {
      ServerQueryRequest queryRequest = currentWinnerGroup.peekFirst().getQueryRequest();
      if (LOGGER.isDebugEnabled()) {
        sb.append(" Winner: " + currentWinnerGroup.name() + ": ["
            + queryRequest.getTimerContext().getQueryArrivalTimeMs() + "," + queryRequest.getRequestId() + ","
            + queryRequest.getSegmentsToQuery().size() + "," + startTime + "]");
      }
      query = currentWinnerGroup.removeFirst();
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(sb.toString());
    }
    return query;
  }

  private void checkGroupHasCapacity(SchedulerGroup groupContext)
      throws OutOfCapacityException {
    if (groupContext.numPending() >= _maxPendingPerGroup
        && groupContext.totalReservedThreads() >= _resourceManager.getTableThreadsHardLimit()) {
      throw new OutOfCapacityException(
          "SchedulerGroup " + groupContext.name() + " is out of capacity. numPending: " + groupContext.numPending()
              + ", maxPending: " + _maxPendingPerGroup + ", reservedThreads: " + groupContext.totalReservedThreads()
              + " threadsHardLimit: " + _resourceManager.getTableThreadsHardLimit());
    }
  }

  private SchedulerGroup getOrCreateGroupContext(String groupName) {
    SchedulerGroup groupContext = _schedulerGroups.get(groupName);
    if (groupContext == null) {
      groupContext = _groupFactory.create(_config, groupName);
      _schedulerGroups.put(groupName, groupContext);
    }
    return groupContext;
  }

  // separate method to allow mocking for unit testing
  private long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @VisibleForTesting
  long getWakeupTimeMicros() {
    return _wakeUpTimeMicros;
  }
}
