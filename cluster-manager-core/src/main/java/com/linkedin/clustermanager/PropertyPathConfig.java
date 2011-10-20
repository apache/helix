package com.linkedin.clustermanager;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PropertyPathConfig
{
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
    //INSTANCE
    addEntry(PropertyType.MESSAGES,2,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES");
    addEntry(PropertyType.MESSAGES,3,"/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}");
    addEntry(PropertyType.CURRENTSTATES,2,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES");
    addEntry(PropertyType.CURRENTSTATES,3,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}");
    addEntry(PropertyType.CURRENTSTATES,4,"/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceGroupName}");
    addEntry(PropertyType.STATUSUPDATES,2,"/{clusterName}/STATUSUPDATES/{instanceName}/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES,4,"/{clusterName}/STATUSUPDATES/{instanceName}/STATUSUPDATES/{subPath}/{recordName}");
    addEntry(PropertyType.ERRORS,2,"/{clusterName}/ERRORS/{instanceName}/ERRORS/");
    addEntry(PropertyType.ERRORS,4,"/{clusterName}/ERRORS/{instanceName}/ERRORS/{subPath}/{recordName}");
    addEntry(PropertyType.HEALTHREPORT,2,"/{clusterName}/HEALTHREPORT/{instanceName}/HEALTHREPORT");
    addEntry(PropertyType.HEALTHREPORT,3,"/{clusterName}/HEALTHREPORT/{instanceName}/HEALTHREPORT/{reportName}");
    //CONTROLLER
    addEntry(PropertyType.MESSAGES_CONTROLLER,1,"/{clusterName}/CONTROLLER/MESSAGES");
    addEntry(PropertyType.MESSAGES_CONTROLLER,2,"/{clusterName}/CONTROLLER/MESSAGES/{msgId}");
    addEntry(PropertyType.ERRORS_CONTROLLER,1,"/{clusterName}/CONTROLLER/ERRORS");
    addEntry(PropertyType.ERRORS_CONTROLLER,2,"/{clusterName}/CONTROLLER/ERRORS/{errorId}");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,1,"/{clusterName}/CONTROLLER/STATUSUPDATES");
    addEntry(PropertyType.STATUSUPDATES_CONTROLLER,2,"/{clusterName}/CONTROLLER/STATUSUPDATES/{statusId}");
    addEntry(PropertyType.LEADER,2,"/{clusterName}/CONTROLLER/LEADER");
    addEntry(PropertyType.HISTORY,2,"/{clusterName}/CONTROLLER/HISTORY");
    addEntry(PropertyType.PAUSE,2,"/{clusterName}/CONTROLLER/PAUSE");
    //@formatter:on

  }
  static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static void addEntry(PropertyType type, int numKeys, String template)
  {
    if (!templateMap.containsKey(type))
    {
      templateMap.put(type, new HashMap<Integer, String>());
    }
    templateMap.get(type).put(numKeys, template);
  }

  public static String getPath(PropertyType type, String clusterName,
      String... keys)
  {
    if (clusterName == null)
    {
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
          System.out.printf("Replacing %s with %s\n", var, clusterName);
          result = result.replace(var, clusterName);
        } else
        {
          System.out.printf("Replacing %s with %s\n", var, keys[count - 2]);
          result = result.replace(var, keys[count - 2]);
        }
      }
    }
    return result;
  }
}