package org.apache.helix.zkscale.zk;

public class ZkScaleException extends RuntimeException {

  private static final long serialVersionUID = 6558251214364526258L; //todo: generate one

  public ZkScaleException(String message) {
    super(message);
  }

  public ZkScaleException(Throwable cause) {
    super(cause);
  }

  public ZkScaleException(String message, Throwable cause) {
    super(message, cause);
  }
}
