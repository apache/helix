/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.helix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.helix.common.ZkTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadLeakageChecker {
  private final static Logger LOG = LoggerFactory.getLogger(ThreadLeakageChecker.class);

  private static ThreadGroup getRootThreadGroup() {
    ThreadGroup candidate = Thread.currentThread().getThreadGroup();
    while (candidate.getParent() != null) {
      candidate = candidate.getParent();
    }
    return candidate;
  }

  private static List<Thread> getAllThreads() {
    ThreadGroup rootThreadGroup = getRootThreadGroup();
    Thread[] threads = new Thread[32];
    int count = rootThreadGroup.enumerate(threads);
    while (count == threads.length) {
      threads = new Thread[threads.length * 2];
      count = rootThreadGroup.enumerate(threads);
    }
    return Arrays.asList(Arrays.copyOf(threads, count));
  }

  private static final String[] ZKSERVER_THRD_PATTERN =
      {"SessionTracker", "NIOServerCxn", "SyncThread:", "ProcessThread"};
  private static final String[] ZKSESSION_THRD_PATTERN =
      new String[]{"ZkClient-EventThread", "ZkClient-AsyncCallback", "-EventThread", "-SendThread"};
  private static final String[] FORKJOIN_THRD_PATTERN = new String[]{"ForkJoinPool"};
  private static final String[] TIMER_THRD_PATTERN = new String[]{"time"};
  private static final String[] TASKSTATEMODEL_THRD_PATTERN = new String[]{"TaskStateModel"};

  /*
   * The two threshold -- warning and limit, are mostly empirical.
   *
   * ZkServer, current version has only 4 threads. In case later version use more, we the limit to 100.
   * The reasoning is that these ZkServer threads are not deemed as leaking no matter how much they have.
   *
   * ZkSession is the ZkClient and native Zookeeper client we have. ZkTestBase has 12 at starting up time.
   * Thus, if there is more than that, it is the test code leaking ZkClient.
   *
   * ForkJoin is created by using parallel stream or similar Java features. This is out of our control.
   * Similar to ZkServer. The limit is to 100 while keep a small _warningLimit.
   *
   * Timer should not happen. Setting limit to 2 not 0 mostly because even when you cancel the timer
   * thread, it may take some not deterministic time for it to go away. So give it some slack here
   *
   * Also note, this ThreadLeakage checker depends on the fact that tests are running sequentially.
   * Otherwise, the report is not going to be accurate.
   */
  private static enum ThreadCategory {
    ZkServer("zookeeper server threads", 4, 100, ZKSERVER_THRD_PATTERN),
    ZkSession("zkclient/zooKeeper session threads", 12, 12, ZKSESSION_THRD_PATTERN),
    ForkJoin("fork join pool threads", 2, 100, FORKJOIN_THRD_PATTERN),
    Timer("timer threads", 0, 2, TIMER_THRD_PATTERN),
    TaskStateModel("TaskStateModel threads", 0, 0, TASKSTATEMODEL_THRD_PATTERN),
    Other("Other threads", 0, 2, new String[]{""});

    private String _description;
    private List<String> _pattern;
    private int _warningLimit;
    private int _limit;

    public String getDescription() {
      return _description;
    }

    public Predicate<String> getMatchPred() {
      if (this.name() != ThreadCategory.Other.name()) {
        Predicate<String> pred = target -> {
          for (String p : _pattern) {
            if (target.toLowerCase().contains(p.toLowerCase())) {
              return true;
            }
          }
          return false;
        };
        return pred;
      }

      List<Predicate<String>> predicateList = new ArrayList<>();
      for (ThreadCategory threadCategory : ThreadCategory.values()) {
        if (threadCategory == ThreadCategory.Other) {
          continue;
        }
        predicateList.add(threadCategory.getMatchPred());
      }
      Predicate<String> pred = target -> {
        for (Predicate<String> p : predicateList) {
          if (p.test(target)) {
            return false;
          }
        }
        return true;
      };

      return pred;
    }

    public int getWarningLimit() {
      return _warningLimit;
    }

    public int getLimit() {
      return _limit;
    }

    private ThreadCategory(String description, int warningLimit, int limit, String[] patterns) {
      _description = description;
      _pattern = Arrays.asList(patterns);
      _warningLimit = warningLimit;
      _limit = limit;
    }
  }

  public static boolean afterClassCheck(String classname) {
    ZkTestBase.reportPhysicalMemory();
    // step 1: get all active threads
    List<Thread> threads = getAllThreads();
    LOG.info(classname + " has active threads cnt:" + threads.size());

    // step 2: categorize threads
    Map<String, List<Thread>> threadByName = null;
    Map<ThreadCategory, Integer> threadByCnt = new HashMap<>();
    Map<ThreadCategory, Set<Thread>> threadByCat = new HashMap<>();
    try {
      threadByName = threads.
          stream().
          filter(p -> p.getThreadGroup() != null && p.getThreadGroup().getName() != null
              &&  ! "system".equals(p.getThreadGroup().getName())).
          collect(Collectors.groupingBy(p -> p.getName()));
    } catch (Exception e) {
      LOG.error("Filtering thread failure with exception:", e);
    }

    threadByName.entrySet().stream().forEach(entry -> {
      String key = entry.getKey(); // thread name
      Arrays.asList(ThreadCategory.values()).stream().forEach(category -> {
        if (category.getMatchPred().test(key)) {
          Integer count = threadByCnt.containsKey(category) ? threadByCnt.get(category) : 0;
          threadByCnt.put(category, count + entry.getValue().size());
          Set<Thread> thisSet = threadByCat.getOrDefault(category, new HashSet<>());
          thisSet.addAll(entry.getValue());
          threadByCat.put(category, thisSet);
        }
      });
    });

    // todo: We should make the following System.out as LOG.INfO once we achieve 0 thread leakage.
    // todo: also the calling point of this method would fail the test
    // step 3: enforce checking policy
    boolean checkStatus = true;
    for (ThreadCategory threadCategory : ThreadCategory.values()) {
      int limit = threadCategory.getLimit();
      int warningLimit = threadCategory.getWarningLimit();

      Integer categoryThreadCnt = threadByCnt.get(threadCategory);
      if (categoryThreadCnt != null) {
        boolean dumpThread = false;
        if (categoryThreadCnt > limit) {
          checkStatus = false;
          LOG.info(
              "Failure " + threadCategory.getDescription() + " has " + categoryThreadCnt + " thread");
          dumpThread = true;
        } else if (categoryThreadCnt > warningLimit) {
          LOG.info(
              "Warning " + threadCategory.getDescription() + " has " + categoryThreadCnt + " thread");
          dumpThread = true;
        } else {
          LOG.info(threadCategory.getDescription() + " has " + categoryThreadCnt + " thread");
        }
        if (!dumpThread) {
          continue;
        }
        // print first 100 thread names
        int i = 0;
        for (Thread t : threadByCat.get(threadCategory)) {
          LOG.debug(i + " thread:" + t.getName());
          i++;
          if (i == 100) {
            LOG.debug(" skipping the rest");
            break;
          }
        }
      }
    }

    return checkStatus;
  }
}
