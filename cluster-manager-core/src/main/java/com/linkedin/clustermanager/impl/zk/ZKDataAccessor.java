package com.linkedin.clustermanager.impl.zk;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.linkedin.clustermanager.core.ClusterDataAccessor;
import com.linkedin.clustermanager.core.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.core.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.model.ClusterView;
import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

public class ZKDataAccessor implements ClusterDataAccessor {
	class ZKPropertyCreateModeCalculator implements
			ClusterDataAccessor.PropertyCreateModeCalculator {

		@Override
		public CreateMode getClusterPropertyCreateMode() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public CreateMode getInstancePropertyCreateMode() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	private static Logger logger = Logger.getLogger(ZKDataAccessor.class);
	private final String _clusterName;
	/**
	 * Cached view of the cluster.
	 */
	private final ClusterView _clusterView;
	private final ZkClient _zkClient;

	public ZKDataAccessor(String clusterName, ZkClient zkClient) {
		this._clusterName = clusterName;
		this._zkClient = zkClient;
		this._clusterView = new ClusterView();
	}

	@Override
	public void setClusterProperty(ClusterPropertyType clusterProperty,
			String key, ZNRecord value) {
		String zkPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
				clusterProperty);
		String targetValuePath = zkPropertyPath + "/" + key;

		if (_zkClient.exists(targetValuePath)) {
			ZKUtil.replaceIfExists(_zkClient, targetValuePath, value);
		}else{
			if(clusterProperty == ClusterPropertyType.LIVEINSTANCES ){
				_zkClient.createEphemeral(targetValuePath, value);
			}else{
				_zkClient.createPersistent(targetValuePath, value);	
			}
		}
		
	}

	@Override
	public void updateClusterProperty(ClusterPropertyType clusterProperty,
			String key, ZNRecord value) {
		String clusterPropertyPath = CMUtil.getClusterPropertyPath(
				_clusterName, clusterProperty);
		String targetValuePath = clusterPropertyPath + "/" + key;

		if (_zkClient.exists(targetValuePath)) {
			ZKUtil.mergeIfExists(_zkClient, targetValuePath, value);
		}
	}

	@Override
	public void setClusterPropertyList(ClusterPropertyType clusterProperty,
			List<ZNRecord> values) {
		// TODO Auto-generated method stub

	}

	@Override
	public ZNRecord getClusterProperty(ClusterPropertyType clusterProperty,
			String key) {
		String clusterPropertyPath = CMUtil.getClusterPropertyPath(
				_clusterName, clusterProperty);
		String targetPath = clusterPropertyPath + "/" + key;

		ZNRecord nodeRecord = _zkClient.readData(targetPath);
		return nodeRecord;
	}

	@Override
	public List<ZNRecord> getClusterPropertyList(
			ClusterPropertyType clusterProperty) {
		// TODO Auto-generated method stub
		String clusterPropertyPath = CMUtil.getClusterPropertyPath(
				_clusterName, clusterProperty);
		List<ZNRecord> children;
		children = ZKUtil.getChildren(_zkClient, clusterPropertyPath);
		_clusterView.setClusterPropertyList(clusterProperty, children);
		return children;
	}

	@Override
	public void setInstanceProperty(String instanceName,
			InstancePropertyType type, String key, final ZNRecord value) {
		String path = CMUtil.getInstancePropertyPath(_clusterName,
				instanceName, type);
		String propertyPath = path + "/" + key;
		boolean exists = _zkClient.exists(propertyPath);
		if (exists) {
			ZKUtil.replaceIfExists(_zkClient, propertyPath, value);
		} else {
			if(type == InstancePropertyType.CURRENTSTATES ){
				_zkClient.createEphemeral(propertyPath, value);
			}else{
				_zkClient.createPersistent(propertyPath, value);
			}
			
		}
	}

	@Override
	public void updateInstanceProperty(String instanceName,
			InstancePropertyType type, String key, ZNRecord value) {
		String path = CMUtil.getInstancePropertyPath(_clusterName,
				instanceName, type) + "/" + key;
		boolean exists = _zkClient.exists(path);
		if (exists) {
			ZKUtil.mergeIfExists(_zkClient, path, value);
		}else{
			if(type == InstancePropertyType.CURRENTSTATES){
				_zkClient.createEphemeral(path, value);
			}
		}
		
	}

	@Override
	public ZNRecord getInstanceProperty(String instanceName,
			InstancePropertyType clusterProperty, String key) {
		String path = CMUtil.getInstancePropertyPath(_clusterName,
				instanceName, clusterProperty);
		String propertyPath = path + "/" + key;
		ZNRecord record = _zkClient.readData(propertyPath, true);
		if (record != null) {
			record = record.extract(key);
		}
		return record;
	}

	@Override
	public List<ZNRecord> getInstancePropertyList(String instanceName,
			InstancePropertyType clusterProperty) {
		String path = CMUtil.getInstancePropertyPath(_clusterName,
				instanceName, clusterProperty);
		return ZKUtil.getChildren(_zkClient, path);
	}

	@Override
	public void removeInstanceProperty(String instanceName,
			InstancePropertyType type, String key) {
		String path = CMUtil.getInstancePropertyPath(_clusterName,
				instanceName, type) + "/" + key;
		if (_zkClient.exists(path)) {
			boolean b = _zkClient.delete(path);
			if (!b) {
				logger.warn("Unable to remove property at path:" + path);
			}
		} else {
			logger.warn("No property to remove at path:" + path);
		}
	}

	@Override
	public void setInstancePropertyList(String instanceName,
			InstancePropertyType clusterProperty, List<ZNRecord> values) {
		// TODO Auto-generated method stub

	}

	public ClusterView getClusterView() {
		return _clusterView;
	}

	@Override
	public void setEphemeralClusterProperty(
			ClusterPropertyType clusterProperty, String key, ZNRecord value) {
		String zkPropertyPath = CMUtil.getClusterPropertyPath(_clusterName,
				clusterProperty);
		String targetValuePath = zkPropertyPath + "/" + key;

		if (_zkClient.exists(targetValuePath)) {
			_zkClient.deleteRecursive(targetValuePath);
		}
		_zkClient.createEphemeral(targetValuePath, value);

	}

	@Override
	public void removeClusterProperty(ClusterPropertyType clusterProperty,
			String key) {
		String path = CMUtil.getClusterPropertyPath(_clusterName,
				clusterProperty) + "/" + key;
		if (_zkClient.exists(path)) {
			boolean b = _zkClient.delete(path);
			if (!b) {
				logger.warn("Unable to remove property at path:" + path);
			}
		} else {
			logger.warn("No property to remove at path:" + path);
		}
	}
}
