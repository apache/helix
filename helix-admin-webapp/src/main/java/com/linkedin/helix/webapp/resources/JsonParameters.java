package com.linkedin.helix.webapp.resources;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.restlet.data.Form;
import org.restlet.resource.Representation;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.tools.ClusterSetup;

public class JsonParameters
{
  // json parameter key
  public static final String             JSON_PARAMETERS     = "jsonParameters";

  // json parameter map keys
  public static final String             PARTITION           = "partition";
  public static final String             RESOURCE            = "resource";
  public static final String             MANAGEMENT_COMMAND  = "command";
  public static final String             ENABLED             = "enabled";
  public static final String             GRAND_CLUSTER       = "grandCluster";
  public static final String             REPLICAS            = "replicas";
  public static final String             RESOURCE_KEY_PREFIX = "key";
  public static final String             INSTANCE_NAME       = "instanceName";
  public static final String             INSTANCE_NAMES      = "instanceNames";
  public static final String             OLD_INSTANCE        = "oldInstance";
  public static final String             NEW_INSTANCE        = "newInstance";
  public static final String             CONFIGS             = "configs";
  public static final String             CLUSTER_NAME        = "clusterName";
  public static final String             PARTITIONS          = "partitions";
  public static final String             RESOURCE_GROUP_NAME = "resourceGroupName";
  public static final String             STATE_MODEL_DEF_REF = "stateModelDefRef";
  public static final String             IDEAL_STATE_MODE    = "mode";

  // aliases for ClusterSetup commands
  public static Map<String, Set<String>> CLUSTERSETUP_COMMAND_ALIASES;
  static
  {
    CLUSTERSETUP_COMMAND_ALIASES = new HashMap<String, Set<String>>();
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.addResource,
                                     new HashSet<String>(Arrays.asList(new String[] { "addResourceGroup" })));
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.activateCluster,
                                     new HashSet<String>(Arrays.asList(new String[] { "enableStorageCluster" })));
    CLUSTERSETUP_COMMAND_ALIASES.put(ClusterSetup.addInstance,
                                     new HashSet<String>(Arrays.asList(new String[] { "addInstance" })));
  }

  // parameter map
  final Map<String, String>              _parameterMap;


  public JsonParameters(Representation entity) throws Exception
  {
    this(new Form(entity));
  }
  
  public JsonParameters(Form form) throws Exception
  {
    // Form form = new Form(entity);
    String jsonPayload = form.getFirstValue(JSON_PARAMETERS, true);
    if (jsonPayload == null || jsonPayload.isEmpty())
    {
      _parameterMap = Collections.emptyMap();
    }
    else
    {
      _parameterMap = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    }
  }

  public String getParameter(String key)
  {
    return _parameterMap.get(key);
  }

  public Map<String, String> cloneParameterMap()
  {
    Map<String, String> parameterMap = new TreeMap<String, String>();
    parameterMap.putAll(_parameterMap);
    return parameterMap;
  }

  public void verifyCommand(String command)
  {

    // verify command itself
    if (_parameterMap.isEmpty())
    {
      throw new HelixException("'" + JSON_PARAMETERS + "' in the POST body is empty");
    }

    if (!_parameterMap.containsKey(MANAGEMENT_COMMAND))
    {
      throw new HelixException("Missing management paramater '" + MANAGEMENT_COMMAND
          + "'");
    }

    if (!_parameterMap.get(MANAGEMENT_COMMAND).equalsIgnoreCase(command)
        && !(CLUSTERSETUP_COMMAND_ALIASES.get(command) != null && CLUSTERSETUP_COMMAND_ALIASES.get(command)
                                                                                              .contains(_parameterMap.get(MANAGEMENT_COMMAND))))
    {
      throw new HelixException(MANAGEMENT_COMMAND + " must be '" + command + "'");
    }

    // verify command parameters
    if (command.equalsIgnoreCase(ClusterSetup.enableInstance))
    {
      if (!_parameterMap.containsKey(ENABLED))
      {
        throw new HelixException("Missing Json parameters: '" + ENABLED + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.enablePartition))
    {
      if (!_parameterMap.containsKey(ENABLED))
      {
        throw new HelixException("Missing Json parameters: '" + ENABLED + "'");
      }

      if (!_parameterMap.containsKey(PARTITION))
      {
        throw new HelixException("Missing Json parameters: '" + PARTITION + "'");
      }

      if (!_parameterMap.containsKey(RESOURCE))
      {
        throw new HelixException("Missing Json parameters: '" + RESOURCE + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.resetPartition))
    {
      if (!_parameterMap.containsKey(PARTITION))
      {
        throw new HelixException("Missing Json parameters: '" + PARTITION + "'");
      }

      if (!_parameterMap.containsKey(RESOURCE))
      {
        throw new HelixException("Missing Json parameters: '" + RESOURCE + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.resetInstance))
    {
      // nothing
    }
    else if (command.equalsIgnoreCase(ClusterSetup.activateCluster))
    {
      if (!_parameterMap.containsKey(GRAND_CLUSTER))
      {
        throw new HelixException("Missing Json parameters: '" + GRAND_CLUSTER + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.setConfig))
    {
      if (!_parameterMap.containsKey(CONFIGS))
      {
        throw new HelixException("Missing Json parameters: '" + CONFIGS + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.removeConfig))
    {
      if (!_parameterMap.containsKey(CONFIGS))
      {
        throw new HelixException("Missing Json parameters: '" + CONFIGS + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.addCluster))
    {
      if (!_parameterMap.containsKey(CLUSTER_NAME))
      {
        throw new HelixException("Missing Json parameters: '" + CLUSTER_NAME + "'");
      }
    }
    else if (command.equalsIgnoreCase(ClusterSetup.addResource))
    {
      if (!_parameterMap.containsKey(RESOURCE_GROUP_NAME))
      {
        throw new HelixException("Missing Json paramaters: '" + RESOURCE_GROUP_NAME + "'");
      }

      if (!_parameterMap.containsKey(PARTITIONS))
      {
        throw new HelixException("Missing Json paramaters: '" + PARTITIONS + "'");
      }

      if (!_parameterMap.containsKey(STATE_MODEL_DEF_REF))
      {
        throw new HelixException("Missing Json paramaters: '" + STATE_MODEL_DEF_REF + "'");
      }

    }
  }

  public String getCommand()
  {
    return _parameterMap.get(MANAGEMENT_COMMAND);
  }

  // temp test
  public static void main(String[] args) throws Exception
  {
    String jsonPayload =
        "{\"command\":\"resetPartition\",\"resource\": \"DB-1\",\"partition\":\"DB-1_22 DB-1_23\"}";
    Map<String, String> map = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    System.out.println(map);
  }
}
