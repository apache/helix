package com.linkedin.helix.webapp.resources;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScope.ConfigScopeProperty;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.webapp.RestAdminApplication;

public class ConfigResource extends Resource
{
  public ConfigResource(Context context, Request request, Response response)
  {
    super(context, request, response);
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setModifiable(true);
  }

  String getValue(String key)
  {
    return (String) getRequest().getAttributes().get(key);
  }

  StringRepresentation getConfigScopes() throws Exception
  {
    StringRepresentation representation = null;
    ZNRecord record = new ZNRecord("Config");

    List<String> scopeList =
        Arrays.asList(ConfigScopeProperty.CLUSTER.toString(),
                      ConfigScopeProperty.RESOURCE.toString(),
                      ConfigScopeProperty.PARTICIPANT.toString(),
                      ConfigScopeProperty.PARTITION.toString());
    record.setListField("scopes", scopeList);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
                                 MediaType.APPLICATION_JSON);

    return representation;
  }

  StringRepresentation getConfigKeys(ConfigScopeProperty scopeProperty, String... keys) throws Exception
  {
    StringRepresentation representation = null;
    String zkServer =
        (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
    String clusterName = getValue("clusterName");

    ClusterSetup setupTool = new ClusterSetup(zkServer);
    HelixAdmin admin = setupTool.getClusterManagementTool();
    ZNRecord record = new ZNRecord(scopeProperty + " Config");

    List<String> configKeys = admin.getConfigKeys(scopeProperty, clusterName, keys);
    record.setListField(scopeProperty.toString(), configKeys);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
                                 MediaType.APPLICATION_JSON);

    return representation;
  }

  StringRepresentation getConfigs(ConfigScope scope,
                                  ConfigScopeProperty scopeProperty,
                                  String... keys) throws Exception
  {
    StringRepresentation representation = null;
    String zkServer =
        (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
    String clusterName = getValue("clusterName");

    ClusterSetup setupTool = new ClusterSetup(zkServer);
    HelixAdmin admin = setupTool.getClusterManagementTool();
    ZNRecord record = new ZNRecord(scopeProperty + " Config");

    List<String> configKeys = admin.getConfigKeys(scopeProperty, clusterName, keys);
    Map<String, String> configs = admin.getConfig(scope, new HashSet<String>(configKeys));
    record.setSimpleFields(configs);

    representation =
        new StringRepresentation(ClusterRepresentationUtil.ZNRecordToJson(record),
                                 MediaType.APPLICATION_JSON);

    return representation;
  }

  @Override
  public Representation represent(Variant variant)
  {
    StringRepresentation representation = null;

    String clusterName = getValue("clusterName");
    String scopeStr = getValue("scope");
    try
    {
      if (scopeStr == null)
      {
        // path is "/clusters/{clusterName}/configs"
        return getConfigScopes();
      }

      scopeStr = scopeStr.toUpperCase();

      ConfigScopeProperty scopeProperty = ConfigScopeProperty.valueOf(scopeStr);
      switch (scopeProperty)
      {
      case CLUSTER:
      case PARTICIPANT:
      case RESOURCE:
        String scopeKey1 = getValue("scopeKey1");
        if (scopeKey1 == null)
        {
          // path is "/clusters/{clusterName}/configs/cluster|participant|resource"
          representation = getConfigKeys(scopeProperty);
        }
        else
        {
          // path is "/clusters/{clusterName}/configs/cluster|particicpant|resource/
          //   {clusterName}|{participantName}|{resourceName}"
          ConfigScope scope;
          if (scopeProperty == ConfigScopeProperty.CLUSTER)
          {
            scope =  new ConfigScopeBuilder().build(scopeProperty, clusterName);
          } else
          {
            scope = new ConfigScopeBuilder().build(scopeProperty, clusterName, scopeKey1);
          }
          representation = getConfigs(scope, scopeProperty, scopeKey1);
        }
        break;
      case PARTITION:
        scopeKey1 = getValue("scopeKey1");
        String scopeKey2 = getValue("scopeKey2");
        if (scopeKey1 == null)
        {
          // path is "/clusters/{clusterName}/configs/partition"
          throw new HelixException("Missing resourceName");
        }
        else if (scopeKey2 == null)
        {
          // path is "/clusters/{clusterName}/configs/partition/resourceName"
          representation = getConfigKeys(scopeProperty, scopeKey1);
        }
        else
        {
          // path is
          // "/clusters/{clusterName}/configs/partition/resourceName/partitionName"
          ConfigScope scope =
              new ConfigScopeBuilder().build(scopeProperty,
                                             clusterName,
                                             scopeKey1,
                                             scopeKey2);
          representation = getConfigs(scope, scopeProperty, scopeKey1, scopeKey2);
        }
        break;
      default:
        break;
      }
    }
    catch (Exception e)
    {
      String error = ClusterRepresentationUtil.getErrorAsJsonStringFromException(e);
      representation = new StringRepresentation(error, MediaType.APPLICATION_JSON);

      e.printStackTrace();
    }

    return representation;
  }

  void setConfigs(Representation entity, String scopeStr) throws Exception
  {
    String zkServer =
        (String) getContext().getAttributes().get(RestAdminApplication.ZKSERVERADDRESS);
    Form form = new Form(entity);
    Map<String, String> jsonParameters =
        ClusterRepresentationUtil.getFormJsonParametersWithCommandVerified(form,
                                                                           ClusterRepresentationUtil._setConfig);

    if (!jsonParameters.containsKey("configs"))
    {
      throw new HelixException("Json parameters does not contain Config values");
    }

    ClusterSetup setupTool = new ClusterSetup(zkServer);
    String propertiesStr = jsonParameters.get("configs");
    setupTool.setConfig(scopeStr, propertiesStr);

    getResponse().setEntity(represent());
    getResponse().setStatus(Status.SUCCESS_OK);
  }

  @Override
  public void acceptRepresentation(Representation entity)
  {
    String clusterName = getValue("clusterName");

    String scopeStr = getValue("scope").toUpperCase();
    try
    {
      ConfigScopeProperty scopeProperty = ConfigScopeProperty.valueOf(scopeStr);

      switch (scopeProperty)
      {
      case CLUSTER:
        String scopeConfigStr =
            ConfigScopeProperty.CLUSTER.toString() + "=" + clusterName;
        setConfigs(entity, scopeConfigStr);
        break;
      case PARTICIPANT:
      case RESOURCE:
        String scopeKey1 = getValue("scopeKey1");

        if (scopeKey1 == null)
        {
          throw new HelixException("Missing resourceName|participantName");
        }
        else
        {
          scopeConfigStr =
              ConfigScopeProperty.CLUSTER.toString() + "=" + clusterName + ","
                  + scopeProperty.toString() + "=" + scopeKey1;

          setConfigs(entity, scopeConfigStr);
        }
        break;
      case PARTITION:
        scopeKey1 = getValue("scopeKey1");
        String scopeKey2 = getValue("scopeKey2");
        if (scopeKey1 == null || scopeKey2 == null)
        {
          throw new HelixException("Missing resourceName|partitionName");
        }
        else
        {
          scopeConfigStr =
              ConfigScopeProperty.CLUSTER.toString() + "=" + clusterName + ","
                  + ConfigScopeProperty.RESOURCE.toString() + "=" + scopeKey1 + ","
                  + scopeProperty.toString() + "=" + scopeKey2;
          setConfigs(entity, scopeConfigStr);
        }
        break;
      default:
        break;
      }
    }
    catch (Exception e)
    {
      getResponse().setEntity(ClusterRepresentationUtil.getErrorAsJsonStringFromException(e),
                              MediaType.APPLICATION_JSON);
      getResponse().setStatus(Status.SUCCESS_OK);
    }
  }

}
