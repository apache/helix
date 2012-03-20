package com.linkedin.helix.tools;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import com.linkedin.helix.DataAccessor;
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
import com.linkedin.helix.manager.file.FileDataAccessor;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.file.FilePropertyStore;

public class ClusterStateVerifier
{
  public static String cluster = "cluster";
  public static String zkServerAddress = "zkSvr";
  public static String help = "help";
  private static Logger LOG = Logger.getLogger(ClusterStateVerifier.class);

  public interface Verifier
  {
    boolean verify();
  }

  /**
   * verifier that verifies best possible state and external view
   */
  public static class BestPossAndExtViewZkVerifier implements Verifier
  {
    private final String zkAddr;
    private final String clusterName;
    private final Map<String, Map<String, String>> errStates;
  
    public BestPossAndExtViewZkVerifier(String zkAddr, String clusterName)
    {
      this(zkAddr, clusterName, null);
    }

    public BestPossAndExtViewZkVerifier(String zkAddr, String clusterName,
        Map<String, Map<String, String>> errStates)
    {
      if (zkAddr == null || clusterName == null)
      {
        throw new IllegalArgumentException("requires zkAddr|clusterName");
      }
      this.zkAddr = zkAddr;
      this.clusterName = clusterName;
      this.errStates = errStates;
    }
    
    @Override
    public boolean verify()
    {      
      ZkClient zkClient = null;
      try
      {
        zkClient = new ZkClient(zkAddr);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

        return ClusterStateVerifier.verifyBestPossAndExtView(accessor, errStates);
      } catch (Exception e)
      {
        LOG.error("exception in verification", e);
        return false;
      } finally
      {
        if (zkClient != null)
        {
          zkClient.close();
        }
      }
    }
    
    @Override
    public String toString()
    {
      String verifierName = getClass().getName();
      verifierName = verifierName.substring(verifierName.lastIndexOf('.') + 1, 
          verifierName.length());
      return verifierName + "(" + zkAddr + ", " + clusterName + ")";
    }
  }

  public static class BestPossAndExtViewFileVerifier implements Verifier
  {
    private final String rootPath;
    private final String clusterName;
    private final Map<String, Map<String, String>> errStates;
    private final FilePropertyStore<ZNRecord> fileStore;
  
    public BestPossAndExtViewFileVerifier(String rootPath, String clusterName)
    {
      this(rootPath, clusterName, null);
    }

    public BestPossAndExtViewFileVerifier(String rootPath, String clusterName,
        Map<String, Map<String, String>> errStates)
    {
      if (rootPath == null || clusterName == null)
      {
        throw new IllegalArgumentException("requires rootPath|clusterName");
      }
      this.rootPath = rootPath;
      this.clusterName = clusterName;
      this.errStates = errStates;
      
      this.fileStore = new FilePropertyStore<ZNRecord>(new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
          rootPath, new PropertyJsonComparator<ZNRecord>(ZNRecord.class));
    }
    
    @Override
    public boolean verify()
    {      
      try
      {
        DataAccessor accessor = new FileDataAccessor(fileStore, clusterName);

        return ClusterStateVerifier.verifyBestPossAndExtView(accessor, errStates);
      } catch (Exception e)
      {
        LOG.error("exception in verification", e);
        return false;
      } finally
      {
      }
    }
    
    @Override
    public String toString()
    {
      String verifierName = getClass().getName();
      verifierName = verifierName.substring(verifierName.lastIndexOf('.') + 1, 
          verifierName.length());
      return verifierName + "(" + rootPath + ", " + clusterName + ")";
    }
  }
  
  static boolean verifyBestPossAndExtView(DataAccessor accessor, 
      Map<String, Map<String, String>> errStates)
  {
    try
    {
      // read cluster once and do verification
      ClusterDataCache cache = new ClusterDataCache();
      cache.refresh(accessor);

      Map<String, IdealState> idealStates = cache.getIdealStates();
      if (idealStates == null || idealStates.isEmpty())
      {
        LOG.debug("No resource idealState");
        return true;
      }

      Map<String, ExternalView> extViews = accessor.getChildValuesMap(ExternalView.class,
          PropertyType.EXTERNALVIEW);
      if (extViews == null || extViews.isEmpty())
      {
        LOG.info("No externalViews");
        return false;
      }

      // calculate best possible state
      BestPossibleStateOutput bestPossOutput = ClusterStateVerifier.calcBestPossState(cache);
      
      // set error states
      if (errStates != null)
      {
        for (String resourceName : errStates.keySet())
        {
          Map<String, String> partErrStates = errStates.get(resourceName);
          for (String partitionName : partErrStates.keySet())
          {
            String instanceName = partErrStates.get(partitionName);
            Map<String, String> partStateMap = bestPossOutput.getInstanceStateMap(resourceName,
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
        Map<Partition, Map<String, String>> bpStateMap = bestPossOutput.getResourceMap(resourceName);
        Iterator iter = bpStateMap.entrySet().iterator();
        while (iter.hasNext())
        {
          Map.Entry pairs = (Map.Entry)iter.next();
          Map<String, String> instanceStateMap = (Map<String, String>) pairs.getValue();
          if (instanceStateMap.isEmpty())
          {
            iter.remove();
          }
        }

        // step 1: externalView and bestPossibleState has equal size
        int extViewSize = extView.getRecord().getMapFields().size() ;
        int bestPossStateSize = bestPossOutput.getResourceMap(resourceName).size();
        if (extViewSize != bestPossStateSize)
        {
          LOG.info("exterView size ("+ extViewSize 
              +") is different from bestPossState size (" + bestPossStateSize +") for resource: "
              + resourceName);
//          System.out.println("extView: " + extView.getRecord().getMapFields());
//          System.out.println("bestPossState: " + bestPossOutput.getResourceMap(resourceName));
          return false;
        }
        
        // step 2: every entry in external view is contained in best possible state
        for (String partition : extView.getRecord().getMapFields().keySet())
        {
          Map<String, String> evInstanceStateMap = extView.getRecord().getMapField(partition);
          Map<String, String> bpInstanceStateMap = bestPossOutput.getInstanceStateMap(resourceName,
              new Partition(partition));

          boolean result = ClusterStateVerifier.<String, String> compareMap(evInstanceStateMap, bpInstanceStateMap);
          if (result == false)
          {
            LOG.info("externalView is different from bestPossibleState for partition:" + partition);
            return false;
          }
        }
      }
    } catch (Exception e)
    {
      LOG.error("exception in verification", e);
      return false;
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
   * calculate the best possible state note that DROPPED states
   * are not checked since when kick off the BestPossibleStateCalcStage we are
   * providing an empty current state map
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

    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE
        .toString());

    // System.out.println("output:" + output);
    return output;
  }
  
  public static <K, V> boolean compareMap(Map<K, V> map1, Map<K, V> map2)
  {
    boolean isEqual = true;
    if (map1 == null && map2 == null)
    {
      // OK
    } else if (map1 == null && map2 != null)
    {
      if (!map2.isEmpty())
      {
        isEqual = false;
      }
    } else if (map1 != null && map2 == null)
    {
      if (!map1.isEmpty())
      {
        isEqual = false;
      }
    } else
    {
      // verify size
      if (map1.size() != map2.size())
      {
        isEqual = false;
      }
      // verify each <key, value> in map1 is contained in map2
      for (K key: map1.keySet())
      {
         if (!map1.get(key).equals(map2.get(key)))
         {
           LOG.debug("different value for key: " + key + "(map1: " + map1.get(key) + ", map2: " + map2.get(key) + ")");
           isEqual = false;
           break;
         }
      }
    }
    return isEqual;
  }
  
  public static boolean verify(Verifier verifier)
  {
    return verify(verifier, 30 * 1000);
  }

  public static boolean verify(Verifier verifier, long timeout)
  {
    final long sleepInterval = 1000; // in ms
    final long loop = timeout / sleepInterval + 1;

    long startTime = System.currentTimeMillis();
    boolean result = false;
    try
    {
      for (int i = 0; i < loop; i++)
      {
        Thread.sleep(sleepInterval);
        result = verifier.verify();
        if (result == true)
        {
          break;
        }
      }
      return result;
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally
    {
      long endTime = System.currentTimeMillis();
      
      // debug
      System.err.println(result + ": " + verifier
          + ": wait " + (endTime - startTime) + "ms to verify");

    }
    return false;
  }

  public static boolean verifyFileBasedClusterStates(String file, String instanceName,
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
        LOG.info("State mismatch--unit_key:" + stateUnitKey + " cur:" + curState + " ideal:"
            + idealState + " instance_name:" + instanceName);
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
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option zkServerOption = OptionBuilder.withLongOpt(zkServerAddress)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption = OptionBuilder.withLongOpt(cluster)
        .withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    return options;
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
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
    if (args.length > 0)
    {
      CommandLine cmd = processCommandLineArgs(args);
      zkServer = cmd.getOptionValue(zkServerAddress);
      clusterName = cmd.getOptionValue(cluster);
    }
    return new BestPossAndExtViewZkVerifier(zkServer, clusterName).verify();

  }

  public static void main(String[] args)
  {
    boolean result = verifyState(args);
    System.out.println(result ? "Successful" : "failed");
  }

}
