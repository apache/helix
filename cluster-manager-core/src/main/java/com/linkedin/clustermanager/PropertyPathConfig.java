package com.linkedin.clustermanager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class PropertyPathConfig
{
  private static Logger logger = Logger.getLogger(PropertyPathConfig.class);

  static Map<PropertyType, Map<Integer, String>> templateMap = new HashMap<PropertyType, Map<Integer, String>>();
  static
  {
    //@formatter:off
    addEntry(PropertyType.CONFIGS,1,"/{clusterName}/CONFIGS");
    addEntry(PropertyType.CONFIGS,2,"/{clusterName}/CONFIGS/{instanceName}");
    addEntry(PropertyType.LIVEINSTANCES,1,"/{clusterName}/LIVEINSTANCES");
    addEntry(PropertyType.LIVEINSTANCES,2,"/{clusterName}/LIVEINSTANCES/{instanceName}");
    addEntry(PropertyType.INSTANCES,1,"/{clusterName}/INSTANCES");
    addEntry(PropertyType.INSTANCES,2,"/{clusterName}/INSTANCES/{instanceName}");
    addEntry(PropertyType.IDEALSTATES,1,"/{clusterName}/IDEALSTATES");
    addEntry(PropertyType.IDEALSTATES,2,"/{clusterName}/IDEALSTATES/{resourceGroupName}");
    addEntry(PropertyType.EXTERNALVIEW,1,"/{clusterName}/EXTERNALVIEW");
    addEntry(PropertyType.EXTERNALVIEW,2,"/{clusterName}/EXTERNALVIEW/{resourceGroupName}");
    addEntry(PropertyType.STATEMODELDEFS,1,"/{clusterName}/STATEMODELDEFS");
    addEntry(PropertyType.STATEMODELDEFS,2,"/{clusterName}/STATEMODELDEFS/{stateModelName}");
    addEntry(PropertyType.CONTROLLER,1,"/{clusterName}/CONTROLLER");
    addEntry(PropertyType.PROPERTYSTORE,1,"/{clusterName}/PROPERTYSTORE");
    //INSTANCE
    addEntry(PropertyType.MESSAGES,2,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES");
    addEntry(PropertyType.MESSAGES,3,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}");
    addEntry(PropertyType.CURRENTSTATES,2,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES");
    addEntry(PropertyType.CURRENTSTATES,3,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}");
    addEntry(PropertyType.CURRENTSTATES,4,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceGroupName}");
    addEntry(PropertyType.STATUSUPDATES,2,"/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES,3,"/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}");
    addEntry(PropertyType.STATUSUPDATES,4,"/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}");
    addEntry(PropertyType.STATUSUPDATES,5,"/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.ERRORS,2,"/{clusterName}/INSTANCES/{instanceName}/ERRORS");
    addEntry(PropertyType.ERRORS,3,"/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}");
    addEntry(PropertyType.ERRORS,4,"/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}");
    addEntry(PropertyType.ERRORS,5,"/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.HEALTHREPORT,2,"/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT");
    addEntry(PropertyType.HEALTHREPORT,3,"/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT/{reportName}");
    //CONTROLLER
    addEntry(PropertyType.MESSAGES_CONTROLLER,1,"/{clusterName}/CONTROLLER/MESSAGES");
    addEntry(PropertyType.MESSAGES_CONTROLLER,2,"/{clusterName}/CONTROLLER/MESSAGES/{msgId}");
    addEntry(PropertyType.ERRORS_CONTROLLER,1,"/{clusterName}/CONTROLLER/ERRORS");
    addEntry(PropertyType.ERRORS_CONTROLLER,2,"/{clusterName}/CONTROLLER/ERRORS/{errorId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,1,"/{clusterName}/CONTROLLER/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,2,"/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,3,"/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}/{subPath}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,4,"/{clusterName}/CONTROLLER/STATUSUPDATES/{sessionId}/{subPath}/{recordName}");
    addEntry(PropertyType.LEADER,1,"/{clusterName}/CONTROLLER/LEADER");
    addEntry(PropertyType.HISTORY,1,"/{clusterName}/CONTROLLER/HISTORY");
    addEntry(PropertyType.PAUSE,1,"/{clusterName}/CONTROLLER/PAUSE");
    addEntry(PropertyType.GLOBALSTATS,1,"/{clusterName}/CONTROLLER/GLOBALSTATS");
    //@formatter:on

  }
  static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static void addEntry(PropertyType type, int numKeys, String template)
  {
    if (!templateMap.containsKey(type))
    {
      templateMap.put(type, new HashMap<Integer, String>());
    }
    logger.info("Adding template for type:" + type.getType() + " arguments:"
        + numKeys + " template:" + template);
    templateMap.get(type).put(numKeys, template);
  }

  public static String getPath(PropertyType type, String clusterName,
      String... keys)
  {
    if (clusterName == null)
    {
      logger.warn("Invalid clusterName:" + clusterName + " for type:" + type);
      return null;
    }
    if (keys == null)
    {
      keys = new String[] {};
    }
    String template = null;
    if (templateMap.containsKey(type))
    {
      // keys.length+1 since we add clusterName
      template = templateMap.get(type).get(keys.length + 1);
    }

    String result = null;

    if (template != null)
    {
      result = template;
      Matcher matcher = pattern.matcher(template);
      int count = 0;
      while (matcher.find())
      {
        count = count + 1;
        String var = matcher.group();
        if (count == 1)
        {
          result = result.replace(var, clusterName);
        } else
        {
          result = result.replace(var, keys[count - 2]);
        }
      }
    }
    if (result == null || result.indexOf('{') > -1 || result.indexOf('}') > -1)
    {
      logger.warn("Unable to instantiate template:" + template
          + " using clusterName:" + clusterName + " and keys:"
          + Arrays.toString(keys));
    }
    return result;
  }
}