/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.tools;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterView;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.pipeline.Stage;
import com.linkedin.helix.controller.pipeline.StageContext;
import com.linkedin.helix.controller.stages.AttributeName;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.BestPossibleStateOutput;
import com.linkedin.helix.controller.stages.ClusterDataCache;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.controller.stages.CurrentStateComputationStage;
import com.linkedin.helix.controller.stages.ResourceComputationStage;
import com.linkedin.helix.manager.file.FileHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.util.ZKClientPool;

public class ClusterStateVerifier
{
  public static String  cluster         = "cluster";
  public static String  zkServerAddress = "zkSvr";
  public static String  help            = "help";
  public static String  timeout         = "timeout";
  public static String  period          = "period";

  private static Logger LOG             = Logger.getLogger(ClusterStateVerifier.class);

  public interface Verifier
  {
    boolean verify();
  }

  public interface ZkVerifier extends Verifier
  {
    ZkClient getZkClient();

    String getClusterName();
  }

  static class ExtViewVeriferZkListener implements IZkChildListener, IZkDataListener
  {
    final CountDownLatch _countDown;
    final ZkClient       _zkClient;
    final Verifier       _verifier;

    public ExtViewVeriferZkListener(CountDownLatch countDown,
                                    ZkClient zkClient,
                                    ZkVerifier verifier)
    {
      _countDown = countDown;
      _zkClient = zkClient;
      _verifier = verifier;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception
    {
      boolean result = _verifier.verify();
      if (result == true)
      {
        _countDown.countDown();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
    {
      for (String child : currentChilds)
      {
        String childPath =
            parentPath.equals("/") ? parentPath + child : parentPath + "/" + child;
        _zkClient.subscribeDataChanges(childPath, this);
      }

      boolean result = _verifier.verify();
      if (result == true)
      {
        _countDown.countDown();
      }
    }

  }

  /**
   * verifier that verifies best possible state and external view
   */
  public static class BestPossAndExtViewZkVerifier implements ZkVerifier
  {
    private final String                           zkAddr;
    private final String                           clusterName;
    private final Map<String, Map<String, String>> errStates;
    private final ZkClient                         zkClient;

    public BestPossAndExtViewZkVerifier(String zkAddr, String clusterName)
    {
      this(zkAddr, clusterName, null);
    }

    public BestPossAndExtViewZkVerifier(String zkAddr,
                                        String clusterName,
                                        Map<String, Map<String, String>> errStates)
    {
      if (zkAddr == null || clusterName == null)
      {
        throw new IllegalArgumentException("requires zkAddr|clusterName");
      }
      this.zkAddr = zkAddr;
      this.clusterName = clusterName;
      this.errStates = errStates;
      this.zkClient = ZKClientPool.getZkClient(zkAddr); // null;
    }

    @Override
    public boolean verify()
    {
      try
      {
        HelixDataAccessor accessor =
            new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

        return ClusterStateVerifier.verifyBestPossAndExtView(accessor, errStates);
      }
      catch (Exception e)
      {
        LOG.error("exception in verification", e);
      }
      return false;
    }

    @Override
    public ZkClient getZkClient()
    {
      return zkClient;
    }

    @Override
    public String getClusterName()
    {
      return clusterName;
    }

    @Override
    public String toString()
    {
      String verifierName = getClass().getName();
      verifierName =
          verifierName.substring(verifierName.lastIndexOf('.') + 1, verifierName.length());
      return verifierName + "(" + clusterName + "@" + zkAddr + ")";
    }
  }

  public static class BestPossAndExtViewFileVerifier implements Verifier
  {
    private final String                           rootPath;
    private final String                           clusterName;
    private final Map<String, Map<String, String>> errStates;
    private final FilePropertyStore<ZNRecord>      fileStore;

    public BestPossAndExtViewFileVerifier(String rootPath, String clusterName)
    {
      this(rootPath, clusterName, null);
    }

    public BestPossAndExtViewFileVerifier(String rootPath,
                                          String clusterName,
                                          Map<String, Map<String, String>> errStates)
    {
      if (rootPath == null || clusterName == null)
      {
        throw new IllegalArgumentException("requires rootPath|clusterName");
      }
      this.rootPath = rootPath;
      this.clusterName = clusterName;
      this.errStates = errStates;

      this.fileStore =
          new FilePropertyStore<ZNRecord>(new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
                                          rootPath,
                                          new PropertyJsonComparator<ZNRecord>(ZNRecord.class));
    }

    @Override
    public boolean verify()
    {
      try
      {
        HelixDataAccessor accessor = new FileHelixDataAccessor(fileStore, clusterName);

        return ClusterStateVerifier.verifyBestPossAndExtView(accessor, errStates);
      }
      catch (Exception e)
      {
        LOG.error("exception in verification", e);
        return false;
      }
      finally
      {
      }
    }

    @Override
    public String toString()
    {
      String verifierName = getClass().getName();
      verifierName =
          verifierName.substring(verifierName.lastIndexOf('.') + 1, verifierName.length());
      return verifierName + "(" + rootPath + "@" + clusterName + ")";
    }
  }

  public static class MasterNbInExtViewVerifier implements ZkVerifier
  {
    private final String   zkAddr;
    private final String   clusterName;
    private final ZkClient zkClient;

    public MasterNbInExtViewVerifier(String zkAddr, String clusterName)
    {
      if (zkAddr == null || clusterName == null)
      {
        throw new IllegalArgumentException("requires zkAddr|clusterName");
      }
      this.zkAddr = zkAddr;
      this.clusterName = clusterName;
      this.zkClient = ZKClientPool.getZkClient(zkAddr);
    }

    @Override
    public boolean verify()
    {
      try
      {
        ZKHelixDataAccessor accessor =
            new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

        return ClusterStateVerifier.verifyMasterNbInExtView(accessor);
      }
      catch (Exception e)
      {
        LOG.error("exception in verification", e);
      }
      return false;
    }

    @Override
    public ZkClient getZkClient()
    {
      return zkClient;
    }

    @Override
    public String getClusterName()
    {
      return clusterName;
    }

  }

  static boolean verifyBestPossAndExtView(HelixDataAccessor accessor,
                                          Map<String, Map<String, String>> errStates)
  {
    try
    {
      Builder keyBuilder = accessor.keyBuilder();
      // read cluster once and do verification
      ClusterDataCache cache = new ClusterDataCache();
      cache.refresh(accessor);

      Map<String, IdealState> idealStates = cache.getIdealStates();
      if (idealStates == null || idealStates.isEmpty())
      {
        LOG.info("No resource idealState");
        return true;
      }

      Map<String, ExternalView> extViews =
          accessor.getChildValuesMap(keyBuilder.externalViews());
      if (extViews == null || extViews.isEmpty())
      {
        LOG.info("No externalViews");
        return false;
      }

      // calculate best possible state
      BestPossibleStateOutput bestPossOutput =
          ClusterStateVerifier.calcBestPossState(cache);

      // set error states
      if (errStates != null)
      {
        for (String resourceName : errStates.keySet())
        {
          Map<String, String> partErrStates = errStates.get(resourceName);
          for (String partitionName : partErrStates.keySet())
          {
            String instanceName = partErrStates.get(partitionName);
            Map<String, String> partStateMap =
                bestPossOutput.getInstanceStateMap(resourceName,
                                                   new Partition(partitionName));
            partStateMap.put(instanceName, "ERROR");
          }
        }
      }

      for (String resourceName : idealStates.keySet())
      {
        ExternalView extView = extViews.get(resourceName);
        if (extView == null)
        {
          LOG.info("externalView for " + resourceName + " is not available");
          return false;
        }

        // step 0: remove empty map from best possible state
        Map<Partition, Map<String, String>> bpStateMap =
            bestPossOutput.getResourceMap(resourceName);
        Iterator<Entry<Partition, Map<String, String>>> iter =
            bpStateMap.entrySet().iterator();
        while (iter.hasNext())
        {
          Map.Entry<Partition, Map<String, String>> entry = iter.next();
          Map<String, String> instanceStateMap = entry.getValue();
          if (instanceStateMap.isEmpty())
          {
            iter.remove();
          }
        }

        // System.err.println("resource: " + resourceName + ", bpStateMap: " +
        // bpStateMap);

        // step 1: externalView and bestPossibleState has equal size
        int extViewSize = extView.getRecord().getMapFields().size();
        int bestPossStateSize = bestPossOutput.getResourceMap(resourceName).size();
        if (extViewSize != bestPossStateSize)
        {
          LOG.info("exterView size (" + extViewSize
              + ") is different from bestPossState size (" + bestPossStateSize
              + ") for resource: " + resourceName);
          // System.out.println("extView: " + extView.getRecord().getMapFields());
          // System.out.println("bestPossState: " +
          // bestPossOutput.getResourceMap(resourceName));
          return false;
        }

        // step 2: every entry in external view is contained in best possible state
        for (String partition : extView.getRecord().getMapFields().keySet())
        {
          Map<String, String> evInstanceStateMap =
              extView.getRecord().getMapField(partition);
          Map<String, String> bpInstanceStateMap =
              bestPossOutput.getInstanceStateMap(resourceName, new Partition(partition));

          boolean result =
              ClusterStateVerifier.<String, String> compareMap(evInstanceStateMap,
                                                               bpInstanceStateMap);
          if (result == false)
          {
            LOG.info("externalView is different from bestPossibleState for partition:"
                + partition);
            return false;
          }
        }
      }
      return true;
    }
    catch (Exception e)
    {
      LOG.error("exception in verification", e);
      return false;
    }

  }

  static boolean verifyMasterNbInExtView(HelixDataAccessor accessor)
  {
    Builder keyBuilder = accessor.keyBuilder();

    Map<String, IdealState> idealStates =
        accessor.getChildValuesMap(keyBuilder.idealStates());
    if (idealStates == null || idealStates.size() == 0)
    {
      LOG.info("No resource idealState");
      return true;
    }

    Map<String, ExternalView> extViews =
        accessor.getChildValuesMap(keyBuilder.externalViews());
    if (extViews == null || extViews.size() < idealStates.size())
    {
      LOG.info("No externalViews | externalView.size() < idealState.size()");
      return false;
    }

    for (String resource : extViews.keySet())
    {
      int partitions = idealStates.get(resource).getNumPartitions();
      Map<String, Map<String, String>> instanceStateMap =
          extViews.get(resource).getRecord().getMapFields();
      if (instanceStateMap.size() < partitions)
      {
        LOG.info("Number of externalViews (" + instanceStateMap.size()
            + ") < partitions (" + partitions + ")");
        return false;
      }

      for (String partition : instanceStateMap.keySet())
      {
        boolean foundMaster = false;
        for (String instance : instanceStateMap.get(partition).keySet())
        {
          if (instanceStateMap.get(partition).get(instance).equalsIgnoreCase("MASTER"))
          {
            foundMaster = true;
            break;
          }
        }
        if (!foundMaster)
        {
          LOG.info("No MASTER for partition: " + partition);
          return false;
        }
      }
    }
    return true;
  }

  static void runStage(ClusterEvent event, Stage stage) throws Exception
  {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  /**
   * calculate the best possible state note that DROPPED states are not checked since when
   * kick off the BestPossibleStateCalcStage we are providing an empty current state map
   * 
   * @param cache
   * @return
   * @throws Exception
   */

  static BestPossibleStateOutput calcBestPossState(ClusterDataCache cache) throws Exception
  {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    event.addAttribute("ClusterDataCache", cache);

    ResourceComputationStage rcState = new ResourceComputationStage();
    CurrentStateComputationStage csStage = new CurrentStateComputationStage();
    BestPossibleStateCalcStage bpStage = new BestPossibleStateCalcStage();

    runStage(event, rcState);
    runStage(event, csStage);
    runStage(event, bpStage);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());

    // System.out.println("output:" + output);
    return output;
  }

  public static <K, V> boolean compareMap(Map<K, V> map1, Map<K, V> map2)
  {
    boolean isEqual = true;
    if (map1 == null && map2 == null)
    {
      // OK
    }
    else if (map1 == null && map2 != null)
    {
      if (!map2.isEmpty())
      {
        isEqual = false;
      }
    }
    else if (map1 != null && map2 == null)
    {
      if (!map1.isEmpty())
      {
        isEqual = false;
      }
    }
    else
    {
      // verify size
      if (map1.size() != map2.size())
      {
        isEqual = false;
      }
      // verify each <key, value> in map1 is contained in map2
      for (K key : map1.keySet())
      {
        if (!map1.get(key).equals(map2.get(key)))
        {
          LOG.debug("different value for key: " + key + "(map1: " + map1.get(key)
              + ", map2: " + map2.get(key) + ")");
          isEqual = false;
          break;
        }
      }
    }
    return isEqual;
  }

  public static boolean verifyByPolling(Verifier verifier)
  {
    return verifyByPolling(verifier, 30 * 1000);
  }

  public static boolean verifyByPolling(Verifier verifier, long timeout)
  {
    return verifyByPolling(verifier, timeout, 1000);
  }

  public static boolean verifyByPolling(Verifier verifier, long timeout, long period)
  {
    long startTime = System.currentTimeMillis();
    boolean result = false;
    try
    {
      long curTime;
      do
      {
        Thread.sleep(period);
        result = verifier.verify();
        if (result == true)
        {
          break;
        }
        curTime = System.currentTimeMillis();
      }
      while (curTime <= startTime + timeout);
      return result;
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    finally
    {
      long endTime = System.currentTimeMillis();

      // debug
      System.err.println(result + ": " + verifier + ": wait " + (endTime - startTime)
          + "ms to verify");

    }
    return false;
  }

  public static boolean verifyByZkCallback(ZkVerifier verifier)
  {
    return verifyByZkCallback(verifier, 30000);
  }

  public static boolean verifyByZkCallback(ZkVerifier verifier, long timeout)
  {
    long startTime = System.currentTimeMillis();
    CountDownLatch countDown = new CountDownLatch(1);
    ZkClient zkClient = verifier.getZkClient();
    String clusterName = verifier.getClusterName();

    
    // add an ephemeral node to /{clusterName}/CONFIGS/CLUSTER/verify
    // so when analyze zk log, we know when a test ends
    zkClient.createEphemeral("/" + clusterName + "/CONFIGS/CLUSTER/verify");
    
    ExtViewVeriferZkListener listener =
        new ExtViewVeriferZkListener(countDown, zkClient, verifier);

    String extViewPath =
        PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);
    zkClient.subscribeChildChanges(extViewPath, listener);
    for (String child : zkClient.getChildren(extViewPath))
    {
      String childPath =
          extViewPath.equals("/") ? extViewPath + child : extViewPath + "/" + child;
      zkClient.subscribeDataChanges(childPath, listener);
    }

    // do initial verify
    boolean result = verifier.verify();
    if (result == false)
    {
      try
      {
        result = countDown.await(timeout, TimeUnit.MILLISECONDS);
        if (result == false)
        {
          // make a final try if timeout
          result = verifier.verify();
        }
      }
      catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // clean up
    zkClient.unsubscribeChildChanges(extViewPath, listener);
    for (String child : zkClient.getChildren(extViewPath))
    {
      String childPath =
          extViewPath.equals("/") ? extViewPath + child : extViewPath + "/" + child;
      zkClient.unsubscribeDataChanges(childPath, listener);
    }

    long endTime = System.currentTimeMillis();

    zkClient.delete("/" + clusterName + "/CONFIGS/CLUSTER/verify");
    // debug
    System.err.println(result + ": wait " + (endTime - startTime) + "ms, " + verifier);

    return result;
  }

  public static boolean verifyFileBasedClusterStates(String file,
                                                     String instanceName,
                                                     StateModelFactory<StateModel> stateModelFactory)
  {
    ClusterView clusterView = ClusterViewSerializer.deserialize(new File(file));
    boolean ret = true;
    int nonOfflineStateNr = 0;

    // ideal_state for instance with name $instanceName
    Map<String, String> instanceIdealStates = new HashMap<String, String>();
    for (ZNRecord idealStateItem : clusterView.getPropertyList(PropertyType.IDEALSTATES))
    {
      Map<String, Map<String, String>> idealStates = idealStateItem.getMapFields();

      for (Map.Entry<String, Map<String, String>> entry : idealStates.entrySet())
      {
        if (entry.getValue().containsKey(instanceName))
        {
          String state = entry.getValue().get(instanceName);
          instanceIdealStates.put(entry.getKey(), state);
        }
      }
    }

    Map<String, StateModel> currentStateMap = stateModelFactory.getStateModelMap();

    if (currentStateMap.size() != instanceIdealStates.size())
    {
      LOG.warn("Number of current states (" + currentStateMap.size() + ") mismatch "
          + "number of ideal states (" + instanceIdealStates.size() + ")");
      return false;
    }

    for (Map.Entry<String, String> entry : instanceIdealStates.entrySet())
    {

      String stateUnitKey = entry.getKey();
      String idealState = entry.getValue();

      if (!idealState.equalsIgnoreCase("offline"))
      {
        nonOfflineStateNr++;
      }

      if (!currentStateMap.containsKey(stateUnitKey))
      {
        LOG.warn("Current state does not contain " + stateUnitKey);
        // return false;
        ret = false;
        continue;
      }

      String curState = currentStateMap.get(stateUnitKey).getCurrentState();
      if (!idealState.equalsIgnoreCase(curState))
      {
        LOG.info("State mismatch--unit_key:" + stateUnitKey + " cur:" + curState
            + " ideal:" + idealState + " instance_name:" + instanceName);
        // return false;
        ret = false;
        continue;
      }
    }

    if (ret == true)
    {
      System.out.println(instanceName + ": verification succeed");
      LOG.info(instanceName + ": verification succeed (" + nonOfflineStateNr + " states)");
    }

    return ret;
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption =
        OptionBuilder.withLongOpt(help)
                     .withDescription("Prints command-line options info")
                     .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServerAddress)
                     .withDescription("Provide zookeeper address")
                     .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster)
                     .withDescription("Provide cluster name")
                     .create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option timeoutOption =
        OptionBuilder.withLongOpt(timeout)
                     .withDescription("Timeout value for verification")
                     .create();
    timeoutOption.setArgs(1);
    timeoutOption.setArgName("Timeout value (Optional), default=30s");

    Option sleepIntervalOption =
        OptionBuilder.withLongOpt(period)
                     .withDescription("Polling period for verification")
                     .create();
    sleepIntervalOption.setArgs(1);
    sleepIntervalOption.setArgName("Polling period value (Optional), default=1s");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(timeoutOption);
    options.addOption(sleepIntervalOption);

    return options;
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    // CommandLine cmd = null;

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    }
    catch (ParseException pe)
    {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static boolean verifyState(String[] args)
  {
    // TODO Auto-generated method stub
    String clusterName = "storage-cluster";
    String zkServer = "localhost:2181";
    long timeoutValue = 0;
    long periodValue = 1000;

    if (args.length > 0)
    {
      CommandLine cmd = processCommandLineArgs(args);
      zkServer = cmd.getOptionValue(zkServerAddress);
      clusterName = cmd.getOptionValue(cluster);
      String timeoutStr = cmd.getOptionValue(timeout);
      String periodStr = cmd.getOptionValue(period);
      if (timeoutStr != null)
      {
        try
        {
          timeoutValue = Long.parseLong(timeoutStr);
        }
        catch (Exception e)
        {
          System.err.println("Exception in converting " + timeoutStr
              + " to long. Use default (0)");
        }
      }

      if (periodStr != null)
      {
        try
        {
          periodValue = Long.parseLong(periodStr);
        }
        catch (Exception e)
        {
          System.err.println("Exception in converting " + periodStr
              + " to long. Use default (1000)");
        }
      }

    }
    // return verifyByPolling(new BestPossAndExtViewZkVerifier(zkServer, clusterName),
    // timeoutValue,
    // periodValue);

    return verifyByZkCallback(new BestPossAndExtViewZkVerifier(zkServer, clusterName),
                              timeoutValue);
  }

  public static void main(String[] args)
  {
    boolean result = verifyState(args);
    System.out.println(result ? "Successful" : "failed");
    System.exit(1);
  }

}
