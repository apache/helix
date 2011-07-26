package com.linkedin.clustermanager.controller;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.util.CMUtil;

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
  private final String _clusterName;

  public ClusterDataProvider(String clusterName)
  {
    this._clusterName = clusterName;
    final ArrayList<String> interestedPath = new ArrayList<String>();
    interestedPath.add(CMUtil.getClusterPropertyPath(clusterName,
        ClusterPropertyType.IDEALSTATES));
    interestedPath.add(CMUtil.getClusterPropertyPath(clusterName,
        ClusterPropertyType.CONFIGS));
    interestedPath.add(CMUtil.getClusterPropertyPath(clusterName,
        ClusterPropertyType.LIVEINSTANCES));
    interestedPath.add(CMUtil.getClusterPropertyPath(clusterName,
        ClusterPropertyType.INSTANCES));
    FileFilter fileFilter = new FileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        String path = pathname.getAbsolutePath();
        for (String temp : interestedPath)
        {
          if (path.startsWith(temp))
          {
            return true;
          }
        }
        return false;
      }
    };
  //  HierarchicalDataHolder<ZNRecord> record = new HierarchicalDataHolder<ZNRecord>();
  }

}
