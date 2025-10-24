package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import java.util.List;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.util.JenkinsHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selection algorithm based on the "straw2" bucket type as described https://www.spinics.net/lists/ceph-devel/msg21635.html.
 */
class Straw2Selector implements Selector {
  private static final Logger LOG = LoggerFactory.getLogger(Straw2Selector.class);

  private final List<Node> _nodes;
  private final JenkinsHash _hashFunction;

  Straw2Selector(Node node) {
    _nodes = node.getChildren();
    _hashFunction = new JenkinsHash();

    if (_nodes == null || _nodes.isEmpty()) {
      LOG.warn("Straw2Selector created with empty node: name={}, type={}, id={}",
          node.getName(), node.getType(), node.getId());
    }
  }

  public Node select(long input, long round) {
    Node selected = null;
    double hiScore = -1;
    for (Node child : _nodes) {
      double score = weightedScore(child, input, round);
      if (score > hiScore) {
        selected = child;
        hiScore = score;
      }
    }
    if (selected == null) {
      throw new IllegalStateException(
          String.format("No node selected for input %d and round %d. NodeCount=%d",
              input, round, _nodes == null ? 0 : _nodes.size()));
    }
    return selected;
  }

  private double weightedScore(Node child, long input, long round) {
    long hash = _hashFunction.hash(input, child.getId(), round);
    hash = hash&0xffff;
    // To avoid doing Math.log of 0 changing hash range from [0..65535] to [1..65535]
    hash = hash == 0 ? 1 : hash;
    if (child.getWeight() > 0) {
      return Math.log(hash/65536d) / child.getWeight();
    }
    return 0.0;
  }
}