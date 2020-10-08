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


public class ThreadLeakageChecker {
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

  private static final String[] ZkServerThrdPattern =
      {"SessionTracker", "NIOServerCxn", "SyncThread:", "ProcessThread"};
  private static final String[] ZkSessionThrdPattern =
      new String[]{"ZkClient-EventThread", "ZkClient-AsyncCallback", "-EventThread", "-SendThread"};
  private static final String[] ForkJoinThrdPattern = new String[]{"ForkJoinPool"};
  private static final String[] TimerThrdPattern = new String[]{"time"};
  private static final String[] TaskStateModelThrdPattern = new String[]{"TaskStateModel"};

  private static enum ThreadCategory {
    ZkServer("zookeeper server threads", 4, 100, ZkServerThrdPattern),
    ZkSession("zkclient/zooKeeper session threads", 12, 12, ZkSessionThrdPattern),
    ForkJoin("fork join pool threads", 2, 10, ForkJoinThrdPattern),
    Timer("timer threads", 0, 2, TimerThrdPattern),
    TaskStateModel("TaskStateModel threads", 0, 0, TaskStateModelThrdPattern),
    Other("Other threads", 0, 3, new String[]{""});

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
    System.out.println(classname + " has active threads cnt:" + threads.size());

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
      System.out.println("filtering thread failure with exception:" + e.getStackTrace());
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

    // step 3: enforce checking policy
    boolean checkStatus = true;
    for (ThreadCategory threadCategory : ThreadCategory.values()) {
      int limit = threadCategory.getLimit();
      int warningLimit = threadCategory.getWarningLimit();

      Integer catThreadCnt = threadByCnt.get(threadCategory);
      if (catThreadCnt != null) {
        boolean dumpThread = false;
        if (catThreadCnt > limit) {
          checkStatus = false;
          System.out.println(
              "Failure " + threadCategory.getDescription() + " has " + catThreadCnt + " thread");
          dumpThread = true;
        } else if (catThreadCnt > warningLimit) {
          System.out.println(
              "Warning " + threadCategory.getDescription() + " has " + catThreadCnt + " thread");
          dumpThread = true;
        } else {
          System.out.println(threadCategory.getDescription() + " has " + catThreadCnt + " thread");
        }
        if (!dumpThread) {
          continue;
        }
        // print first 10 thread names
        int i = 0;
        for (Thread t : threadByCat.get(threadCategory)) {
          System.out.println(i + " thread:" + t.getName());
          i++;
          if (i == 100) {
            System.out.println(" skipping the rest");
            break;
          }
        }
      }
    }

    return checkStatus;
  }
}
