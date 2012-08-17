package com.linkedin.helix.manager.zk;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

public interface PathBasedZkSerializer {

    public byte[] serialize(Object data, String path) throws ZkMarshallingError;

    public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError;

}
