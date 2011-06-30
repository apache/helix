package com.linkedin.clustermanager.agent.zk;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.CMConstants.ChangeType;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.util.CMUtil;

import static com.linkedin.clustermanager.CMConstants.ChangeType.*;

public class ZKClusterManager implements ClusterManager {
	private static Logger logger = Logger.getLogger(ZKClusterManager.class);
	private static final int RETRY_LIMIT = 3;
	private static final int CONNECTIONTIMEOUT = 10000;
	private final String _clusterName;
	private final String _instanceName;
	private final String _zkConnectString;
	private static int SESSIONTIMEOUT = 3000;
	private ZKDataAccessor _accessor;
	private ZkClient _zkClient;
	private List<CallbackHandler> _handlers;
	private final ZkStateChangeListener _zkStateChangeListener;
	private final InstanceType _instanceType;
	private String _sessionId;
	

	public ZKClusterManager(String clusterName, String instanceName,
			InstanceType instanceType, String zkConnectString) throws Exception {
		_clusterName = clusterName;
		_instanceName = instanceName;
		this._instanceType = instanceType;
		_zkConnectString = zkConnectString;
		_zkStateChangeListener = new ZkStateChangeListener();
		connect();
	}

	private boolean isInstanceSetup() {
		if (_instanceType == InstanceType.PARTICIPANT) {
			boolean isValid = _zkClient.exists(CMUtil.getConfigPath(
					_clusterName, _instanceName))
					&& _zkClient.exists(CMUtil.getMessagePath(_clusterName,
							_instanceName))
					&& _zkClient.exists(CMUtil.getCurrentStatePath(
							_clusterName, _instanceName))
					&& _zkClient.exists(CMUtil.getStatusUpdatesPath(
							_clusterName, _instanceName))
					&& _zkClient.exists(CMUtil.getErrorsPath(_clusterName,
							_instanceName));
			return isValid;
		}
		return true;
	}

	public ZKClusterManager(String clusterName, InstanceType instanceType,
			String zkConnectString) throws Exception {
		this(clusterName, null, instanceType, zkConnectString);
	}

	@Override
	public void addIdealStateChangeListener(
			final IdealStateChangeListener listener) throws Exception {
		final String path = CMUtil.getIdealStatePath(_clusterName);
		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeDataChanged,
						EventType.NodeDeleted, EventType.NodeCreated },
				IDEAL_STATE);
		_zkClient.subscribeChildChanges(path, callbackHandler);

	}

	@Override
	public void addLiveInstanceChangeListener(
			LiveInstanceChangeListener listener) {
		final String path = CMUtil.getLiveInstancesPath(_clusterName);
		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeChildrenChanged,
						EventType.NodeDeleted, EventType.NodeCreated },
				LIVE_INSTANCE);
		_zkClient.subscribeChildChanges(path, callbackHandler);

	}

	@Override
	public void addConfigChangeListener(ConfigChangeListener listener) {
		final String path = CMUtil.getConfigPath(_clusterName);

		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeChildrenChanged }, CONFIG);
		_zkClient.subscribeChildChanges(path, callbackHandler);
	}

	@Override
	public void addMessageListener(MessageListener listener, String instanceName) {
		final String path = CMUtil.getMessagePath(_clusterName, instanceName);

		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeChildrenChanged,
						EventType.NodeDeleted, EventType.NodeCreated }, MESSAGE);
		_zkClient.subscribeChildChanges(path, callbackHandler);
	}

	@Override
	public void addCurrentStateChangeListener(
			CurrentStateChangeListener listener, String instanceName) {
		final String path = CMUtil.getCurrentStatePath(_clusterName,
				instanceName);

		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeChildrenChanged,
						EventType.NodeDeleted, EventType.NodeCreated },
				CURRENT_STATE);
		_zkClient.subscribeChildChanges(path, callbackHandler);
	}

	@Override
	public void addExternalViewChangeListener(
			ExternalViewChangeListener listener) {
		final String path = CMUtil.getExternalViewPath(_clusterName);

		CallbackHandler callbackHandler = createCallBackHandler(path, listener,
				new EventType[] { EventType.NodeDataChanged,
						EventType.NodeDeleted, EventType.NodeCreated },
				EXTERNAL_VIEW);
		_zkClient.subscribeChildChanges(path, callbackHandler);
	}

	@Override
	public ClusterDataAccessor getDataAccessor() {
		return _accessor;
	}

	@Override
	public String getClusterName() {
		return _clusterName;
	}

	@Override
	public String getInstanceName() {
		return _instanceName;
	}

	@Override
	public void connect() throws Exception {
		if (_zkStateChangeListener.isConnected()) {
			return;
		}
		_zkClient = createClient(_zkConnectString, SESSIONTIMEOUT);
		_accessor = new ZKDataAccessor(_clusterName, _zkClient);
		if (!isClusterSetup()) {
			throw new Exception(
					"Initial cluster structure is not set up for cluster:"
							+ _clusterName);
		}
		if (!isInstanceSetup()) {
			throw new Exception(
					"Initial cluster structure is not set up for instance:"
							+ _instanceName + " instanceType:" + _instanceType);
		}
		if (_instanceType == InstanceType.PARTICIPANT) {
			addLiveInstance();
		}
	}

	@Override
	public void disconnect() {
		_zkClient.close();
	}

	@Override
	public String getSessionId() {
		return _sessionId;
	}

	private void addLiveInstance() {
		ZNRecord metaData = new ZNRecord();
		// set it from the session
		metaData.setId(_instanceName);
		metaData.setSimpleField(CMConstants.ZNAttribute.SESSION_ID.toString(),
				_sessionId);
		_accessor.setEphemeralClusterProperty(
				ClusterPropertyType.LIVEINSTANCES, _instanceName, metaData);
	}

	private ZkClient createClient(String zkServers, int sessionTimeout)
			throws Exception {
		ZkSerializer zkSerializer = new ZNRecordSerializer();
		ZkClient client = new ZkClient(zkServers, sessionTimeout,
				CONNECTIONTIMEOUT, zkSerializer);
		_sessionId = UUID.randomUUID().toString();
		int retryCount = 0;
		client.subscribeStateChanges(_zkStateChangeListener);
		while (retryCount < RETRY_LIMIT) {
			try {
				client.waitUntilConnected(sessionTimeout, TimeUnit.MILLISECONDS);
				break;
			} catch (Exception e) {
				retryCount++;
				// log
				if (retryCount == RETRY_LIMIT) {
					throw e;
				}
			}
		}
		return client;
	}

	private CallbackHandler createCallBackHandler(String path, Object listener,
			EventType[] eventTypes, ChangeType changeType) {

		return new CallbackHandler(this, _zkClient, path, listener, eventTypes,
				changeType);
	}

	private boolean isClusterSetup() {
		String idealStatePath = CMUtil.getIdealStatePath(_clusterName);
		boolean isValid = _zkClient.exists(idealStatePath)
				&& _zkClient.exists(CMUtil.getConfigPath(_clusterName))
				&& _zkClient.exists(CMUtil.getLiveInstancesPath(_clusterName))
				&& _zkClient
						.exists(CMUtil.getMemberInstancesPath(_clusterName))
				&& _zkClient.exists(CMUtil.getExternalViewPath(_clusterName));
		return isValid;
	}

	@Override
	public boolean isConnected() {
		return _zkStateChangeListener.isConnected();
	}

}
