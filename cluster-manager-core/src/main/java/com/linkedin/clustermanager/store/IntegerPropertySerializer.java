package com.linkedin.clustermanager.store;


public class IntegerPropertySerializer implements PropertySerializer<Integer>
{

  @Override
  public byte[] serialize(Integer data) throws PropertyStoreException
  {
    if (data == null)
    {
      return null;
    }
    
    return data.toString().getBytes();
  }

  @Override
  public Integer deserialize(byte[] bytes) throws PropertyStoreException
  {
    if (bytes == null)
    {
      return null;
    }
    
    try 
    {
      Integer i = Integer.valueOf(new String(bytes));
      return i;
    }
    catch (NumberFormatException e)
    {
      throw new PropertyStoreException(e.getMessage());
    }
  }

}
