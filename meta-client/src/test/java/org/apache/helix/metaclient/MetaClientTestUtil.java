package org.apache.helix.metaclient;

import java.util.concurrent.TimeUnit;


public class MetaClientTestUtil {
  public static final long WAIT_DURATION =  TimeUnit.MINUTES.toMicros(1);
  public interface Verifier {
    boolean verify()
        throws Exception;
  }

  public static boolean verify(Verifier verifier, long timeout)
      throws Exception {
    long start = System.currentTimeMillis();
    do {
      boolean result = verifier.verify();
      if (result || (System.currentTimeMillis() - start) > timeout) {
        return result;
      }
      Thread.sleep(50);
    } while (true);
  }

}
