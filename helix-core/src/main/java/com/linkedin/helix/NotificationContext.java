package com.linkedin.helix;

import java.util.HashMap;
import java.util.Map;

public class NotificationContext
{
  private Map<String, Object> _map;

  private HelixManager _manager;
  private Type _type;
  private String _pathChanged;
  private String _eventName;

  public String getEventName()
  {
    return _eventName;
  }

  public void setEventName(String eventName)
  {
    _eventName = eventName;
  }

  public NotificationContext(HelixManager manager)
  {
    this._manager = manager;
    _map = new HashMap<String, Object>();
  }

  public HelixManager getManager()
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

  public void setManager(HelixManager manager)
  {
    this._manager = manager;
  }

  public void add(String key, Object value)
  {
    _map.put(key, value);
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
    INIT, CALLBACK, FINALIZE
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
