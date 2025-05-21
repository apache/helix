package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import java.util.List;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.util.JenkinsHash;

/**
 * Selection algorithm based on the "straw2" bucket type as described https://www.spinics.net/lists/ceph-devel/msg21635.html.
 */
class Straw2Selector implements Selector {

  private final List<Node> _nodes;
  private final JenkinsHash _hashFunction;

  Straw2Selector(Node node) {
    _nodes = node.getChildren();
    _hashFunction = new JenkinsHash();
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
    return selected;
  }

  private double weightedScore(Node child, long input, long round) {
    long hash = _hashFunction.hash(input, child.getId(), round);
    hash = hash&0xffff;
    if (child.getWeight() > 0) {
      return Math.log(hash/65536d) / child.getWeight();
    }
    return 0.0;
  }
}

