package com.linkedin.clustermanager.controller;

import java.io.FileFilter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.agent.zk.ZkClient;

/**
 * Stores the state of the cluster.
 * 
 * Optimizes read performance by reading data only if the data has changed
 * 
 * @author kgopalak
 * 
 */
public class ClusterDataProvider
{

}



