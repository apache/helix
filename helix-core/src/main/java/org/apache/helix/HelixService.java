package org.apache.helix;

/**
 * Operational methods of a helix role
 */
public interface HelixService {
  /**
   * start helix service async
   */
  void startAsync();

  /**
   * stop helix service async
   */
  void stopAsync();
}
