package com.linkedin.clustermanager.impl.zk;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.core.ClusterManagementTool;
import com.linkedin.clustermanager.core.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKClusterManagementTool implements ClusterManagementTool {
	
	private ZkClient _zkClient;

    private static Logger logger = Logger.getLogger(ZKClusterManagementTool.class);
	
    public ZKClusterManagementTool(ZkClient zkClient)
    {
    	_zkClient = zkClient;
    }
    
    @Override
    public void addNode(String clusterName, ZNRecord nodeConfig)
    {
        String instanceConfigPath = CMUtil.getConfigPath(clusterName);
        String nodeId = nodeConfig.getId();
        ZKUtil.createChildren(_zkClient, instanceConfigPath, nodeConfig);

        _zkClient.createPersistent(CMUtil.getMessagePath(clusterName, nodeId),
                true);
        _zkClient.createPersistent(
                CMUtil.getCurrentStatePath(clusterName, nodeId), true);
        _zkClient.createPersistent(CMUtil.getErrorsPath(clusterName, nodeId),
                true);
        _zkClient.createPersistent(
                CMUtil.getStatusUpdatesPath(clusterName, nodeId), true);
    }

    public ZNRecord getNodeConfig(String nodeId)
    {
        return null;
    }


	@Override
	public void shutdownInstance(String clusterName, String instanceName) 
	{
	}
    @Override
    public void addCluster(String clusterName, boolean overwritePrevRecord)
    {
        // TODO Auto-generated method stub
        String root = "/" + clusterName;

        // TODO For ease of testing only, should remove later
        if (_zkClient.exists(root))
        {
            logger.warn("Root directory exists.Cleaning the root directory:"
                    + root + " overwritePrevRecord: " + overwritePrevRecord);
            if (overwritePrevRecord)
            {
                _zkClient.deleteRecursive(root);
            }
            else
            {
                return;
            }
        }

        _zkClient.createPersistent(root);
        // IDEAL STATE
        _zkClient.createPersistent(CMUtil.getIdealStatePath(clusterName));
        // CONFIGURATIONS
        _zkClient.createPersistent(CMUtil.getConfigPath(clusterName));
        // LIVE INSTANCES
        _zkClient.createPersistent(CMUtil.getLiveInstancesPath(clusterName));
        // MEMBER INSTANCES
        _zkClient.createPersistent(CMUtil.getMemberInstancesPath(clusterName));
        // External view
        _zkClient.createPersistent(CMUtil.getExternalViewPath(clusterName));

    }

    public List<String> getNodeNamesInCluster(String clusterName)
    {
        String memberInstancesPath = CMUtil
                .getMemberInstancesPath(clusterName);
        return _zkClient.getChildren(memberInstancesPath);
    }

    public void addDatabase(String clusterName, String dbName, int partitions)
    {
        ZNRecord dbEmptyIdealState = new ZNRecord();
        dbEmptyIdealState.setId(dbName);
        dbEmptyIdealState.setSimpleField("partitions",
                String.valueOf(partitions));

        String idealStatePath = CMUtil.getIdealStatePath(clusterName);
        String dbIdealStatePath = idealStatePath + "/" + dbName;
        if (_zkClient.exists(dbIdealStatePath))
        {
            logger.warn("Skip the operation. DB ideal state directory exists:"
                    + dbIdealStatePath);
            return;
        }

        ZKUtil.createChildren(_zkClient, idealStatePath, dbEmptyIdealState);
    }

    public List<String> getClusters()
    {
        return _zkClient.getChildren("/");
    }

    public List<String> getDatabasesInCluster(String clusterName)
    {
        return _zkClient.getChildren(CMUtil.getIdealStatePath(clusterName));
    }

    @Override
    public ZNRecord getDBIdealState(String clusterName, String dbName)
    {
      return new ZKDataAccessor(clusterName, _zkClient).getClusterProperty(ClusterPropertyType.IDEALSTATES, dbName);
    }

    @Override
    public void setDBIdealState(String clusterName, String dbName, ZNRecord idealState)
    {
      new ZKDataAccessor(clusterName, _zkClient).setClusterProperty(ClusterPropertyType.IDEALSTATES, dbName, idealState);
    }
}
