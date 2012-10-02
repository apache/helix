package com.linkedin.helix.manager.zk;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

public interface PathBasedZkSerializer
{

  /**
   * Serialize data differently according to different paths
   * 
   * @param data
   * @param path
   * @return
   * @throws ZkMarshallingError
   */
  public byte[] serialize(Object data, String path) throws ZkMarshallingError;

  /**
   * Deserialize data differently according to different paths
   * 
   * @param bytes
   * @param path
   * @return
   * @throws ZkMarshallingError
   */
  public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError;

}
