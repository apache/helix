package org.apache.helix;

import org.apache.zookeeper.CreateMode;

public class AccessOption
{
  public static int PERSISTENT = 0x1;
  public static int EPHEMERAL = 0x2;
  public static int PERSISTENT_SEQUENTIAL = 0x4;
  public static int EPHEMERAL_SEQUENTIAL = 0x8;
  public static int THROW_EXCEPTION_IFNOTEXIST = 0x10;

  /**
   * Helper method to get zookeeper create mode from options
   * 
   * @param options
   * @return zookeeper create mode
   */
  public static CreateMode getMode(int options)
  {
    if ((options & PERSISTENT) > 0)
    {
      return CreateMode.PERSISTENT;
    }
    else if ((options & EPHEMERAL) > 0)
    {
      return CreateMode.EPHEMERAL;
    }
    else if ((options & PERSISTENT_SEQUENTIAL) > 0)
    {
      return CreateMode.PERSISTENT_SEQUENTIAL;
    }
    else if ((options & EPHEMERAL_SEQUENTIAL) > 0)
    {
      return CreateMode.EPHEMERAL_SEQUENTIAL;
    }

    return null;
  }

  /**
   * Helper method to get is-throw-exception-on-node-not-exist from options
   * 
   * @param options
   * @return
   */
  public static boolean isThrowExceptionIfNotExist(int options)
  {
    return (options & THROW_EXCEPTION_IFNOTEXIST) > 0;
  }

}
