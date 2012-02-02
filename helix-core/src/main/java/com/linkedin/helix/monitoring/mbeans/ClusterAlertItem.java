package com.linkedin.helix.monitoring.mbeans;

public class ClusterAlertItem implements ClusterAlertItemMBean
{
  public ClusterAlertItem(String name, int value)
  {
    _alertItemName = name;
    _alertValue = value;
  }
  String _alertItemName;
  int _alertValue;
  @Override
  public String getAlertName()
  {
    return _alertItemName;
  }

  @Override
  public int getAlertValue()
  {
    return _alertValue;
  }
  
  void setValue(int value)
  {
    _alertValue = value;
  }
}
