package org.apache.helix.controller.rebalancer.condition;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;

/**
 * The {@code RebalanceCondition} interface defines a condition under which a rebalance operation
 * should be performed. Implementations of this interface provide specific criteria to determine
 * whether a rebalance is necessary based on the current state of the system.
 */
public interface RebalanceCondition {
  /**
   * Determines whether a rebalance should be performed based on the provided
   * {@link ResourceControllerDataProvider} cache data.
   *
   * @param cache the {@code ResourceControllerDataProvider} cached data of the resources being managed.
   * @return {@code true} if the rebalance should be performed, {@code false} otherwise.
   */
  boolean shouldPerformRebalance(ResourceControllerDataProvider cache);
}
