package com.linkedin.clustermanager.controller;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;

import com.linkedin.clustermanager.PropertyType;
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
    interestedPath.add(CMUtil.getPropertyPath(clusterName,
        PropertyType.IDEALSTATES));
    interestedPath.add(CMUtil.getPropertyPath(clusterName,
        PropertyType.CONFIGS));
    interestedPath.add(CMUtil.getPropertyPath(clusterName,
        PropertyType.LIVEINSTANCES));
    interestedPath.add(CMUtil.getPropertyPath(clusterName,
        PropertyType.INSTANCES));
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
