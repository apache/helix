package com.linkedin.helix.store.zk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.data.Stat;

public class ZNode
{
  // used in write through cache where don't cache stat
  public static final Stat DUMMY_STAT = new Stat();  
  
  final String _zkPath;
  private Stat _stat;
  Object _data;
  Set<String> _childSet;

  public ZNode(String zkPath, Object data, Stat stat)
  {
    _zkPath = zkPath;
    _childSet = new HashSet<String>();
    _data = data;
    _stat = stat;
  }

  public void removeChild(String child)
  {
    _childSet.remove(child);
  }
  
  public void addChild(String child)
  {
    _childSet.add(child);
  }
  
  public void addChildren(List<String> children)
  {
    _childSet.addAll(children);
  }

  public boolean hasChild(String child)
  {
    return _childSet.contains(child);
  }

  public Set<String> getChild()
  {
    return _childSet;
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
    return _zkPath + ", " + _data + ", " + _childSet + ", " + _stat;
  }
}
