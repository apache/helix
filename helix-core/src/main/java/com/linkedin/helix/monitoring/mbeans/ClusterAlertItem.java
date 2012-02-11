package com.linkedin.helix.monitoring.mbeans;


import com.linkedin.helix.alerts.AlertValueAndStatus;

public class ClusterAlertItem implements ClusterAlertItemMBean
{
  String _alertItemName;
  double  _alertValue;
  int _alertFired;
  AlertValueAndStatus _valueAndStatus;
  
  public ClusterAlertItem(String name, AlertValueAndStatus valueAndStatus)
  {
    _valueAndStatus = valueAndStatus;
    _alertItemName = name;
    refreshValues();
  }
  @Override
  public String getSensorName()
  {
    return _alertItemName;
  }

  @Override
  public double getAlertValue()
  {
    return _alertValue;
  }
  
  public void setValueMap(AlertValueAndStatus valueAndStatus)
  {
    _valueAndStatus = valueAndStatus;
    refreshValues();
  }
  
  void refreshValues()
  {
    if(_valueAndStatus.getValue().getElements().size() > 0)
    {
      _alertValue = Double.parseDouble(_valueAndStatus.getValue().getElements().get(0));
    }
    else
    {
      _alertValue = 0;
    }
    _alertFired = _valueAndStatus.isFired() ?  1 : 0;
  }
  @Override
  public int getAlertFired()
  {
    return _alertFired;
  }
}
