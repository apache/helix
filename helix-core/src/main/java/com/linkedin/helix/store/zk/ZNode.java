package com.linkedin.helix.store.zk;

import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.data.Stat;

public class ZNode
{
  final String _name;
  private Stat _stat;
  Object _data;
  Set<String> _childSet;

  public ZNode(String name, Object data, Stat stat)
  {
    _name = name;
    _childSet = new HashSet<String>();
    _data = data;
    _stat = stat;
  }

  public void addChild(String child)
  {
    _childSet.add(child);
  }

  public boolean hasChild(String child)
  {
    return _childSet.contains(child);
  }

  public void setData(Object data)
  {
    _data= data;    
  }
  
  public Object getData()
  {
    return _data;
  }
  
  public void setStat(Stat stat)
  {
    _stat = stat;
  }
  
  public Stat getStat()
  {
    return _stat;
  }
  
  @Override
  public String toString()
  {
    return _name + ", " + _data + ", " + _childSet + ", " + _stat;
  }
}
