package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;
import java.util.List;

public interface Selector {
  enum StrawBucket {
    STRAW,
    STRAW2
  }
  Node select(long input, long round);
}
