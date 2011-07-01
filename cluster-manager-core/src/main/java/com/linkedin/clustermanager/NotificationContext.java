package com.linkedin.clustermanager;

import java.util.HashMap;
import java.util.Map;

public class NotificationContext
{
  private Map<String, Object> _map;

  private ClusterManager _manager;
  private Type _type;
  private String _pathChanged;

  public NotificationContext(ClusterManager manager)
  {
    this._manager = manager;
    _map = new HashMap<String, Object>();
  }

  public ClusterManager getManager()
  {
    return _manager;
  }

  public Map<String, Object> getMap()
  {
    return _map;
  }

  public Type getType()
  {
    return _type;
  }

  public void setManager(ClusterManager manager)
  {
    this._manager = manager;
  }

  public void add(String key, Object name)
  {

  }

  public void setMap(Map<String, Object> map)
  {
    this._map = map;
  }

  public void setType(Type type)
  {
    this._type = type;
  }

  public Object get(String key)
  {
    return _map.get(key);
  }

  public enum Type
  {
    INIT, CALLBACK
  }

  public String getPathChanged()
  {
    return _pathChanged;
  }

  public void setPathChanged(String pathChanged)
  {
    this._pathChanged = pathChanged;
  }
}
