package com.linkedin.helix.model;

/**
 * The name of a resource
 */
public class Partition
{
  private final String _partitionName;

  public String getPartitionName()
  {
    return _partitionName;
  }

  public Partition(String partitionName)
  {
    this._partitionName = partitionName;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null || !(obj instanceof Partition)){
      return false;
    }

    Partition that = (Partition)obj;
    return _partitionName.equals(that.getPartitionName());
  }

  @Override
  public int hashCode()
  {
    return _partitionName.hashCode();
  }

  @Override
  public String toString()
  {
    return _partitionName;
  }
}
