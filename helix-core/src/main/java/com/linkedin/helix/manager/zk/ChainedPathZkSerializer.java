package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ChainedPathZkSerializer implements PathBasedZkSerializer
{

  public static class Builder
  {
    private final ZkSerializer _defaultSerializer;
    private List<ChainItem> _items = new ArrayList<ChainItem>();

    private Builder(ZkSerializer defaultSerializer)
    {
      _defaultSerializer = defaultSerializer;
    }

    /**
     * Add a serializing strategy for the given path prefix
     */
    public Builder serialize(String path, ZkSerializer withSerializer)
    {
      _items.add(new ChainItem(path, withSerializer));
      return this;
    }
    
    /**
     * Builds the serializer with the given strategies and default serializer.
     */
    public ChainedPathZkSerializer build() {
      return new ChainedPathZkSerializer(_defaultSerializer, _items);
    }
  }
  
  /**
   * Create a builder that will use the given serializer by default
   * if no other strategy is given to solve the path in question.
   */
  public static Builder builder(ZkSerializer defaultSerializer) 
  {
    return new Builder(defaultSerializer);
  }

  private final List<ChainItem> _items;
  private final ZkSerializer _defaultSerializer;

  public ChainedPathZkSerializer(ZkSerializer defaultSerializer, List<ChainItem> items)
  {
    _items = items;
    _defaultSerializer = defaultSerializer;
  }

  @Override
  public byte[] serialize(Object data, String path) throws ZkMarshallingError
  {
    for (ChainItem item : _items)
    {
      if (item.matches(path)) return item._serializer.serialize(data);
    }
    return _defaultSerializer.serialize(data);
  }

  @Override
  public Object deserialize(byte[] bytes, String path)
      throws ZkMarshallingError
  {
    for (ChainItem item : _items)
    {
      if (item.matches(path)) return item._serializer.deserialize(bytes);
    }
    return _defaultSerializer.deserialize(bytes);
  }

  private static class ChainItem
  {
    final String _path;
    final ZkSerializer _serializer;

    ChainItem(String path, ZkSerializer serializer)
    {
      _path = path;
      _serializer = serializer;
    }

    boolean matches(String path)
    {
      if (_path.equals(path))
      {
        return true;
      } 
      else if (path.length() > _path.length())
      {
        if (path.startsWith(_path) && path.charAt(_path.length()) == '/') 
        {
          return true;
        }
      }
      return false;
    }
  }

}
