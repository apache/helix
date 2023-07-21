package org.apache.helix.metaclient;

public class MetaClientTestUtil {
  public static final long WAIT_DURATION = 60 * 1000L;
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
