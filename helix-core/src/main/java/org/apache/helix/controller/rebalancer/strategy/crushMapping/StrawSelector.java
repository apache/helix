package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.util.JenkinsHash;

/**
 * Selection algorithm based on the "straw" bucket type as described in the CRUSH algorithm.
 */
class StrawSelector implements Selector {

  private final Map<Node,Long> _straws = new HashMap<Node,Long>();
  private final JenkinsHash _hashFunction;

  StrawSelector(Node node) {
    if (!node.isLeaf()) {
      // create a map from the nodes to their values
      List<Node> sortedNodes = sortNodes(node.getChildren()); // do a reverse sort by weight

      int numLeft = sortedNodes.size();
      float straw = 1.0f;
      float wbelow = 0.0f;
      float lastw = 0.0f;
      int i = 0;
      final int length = sortedNodes.size();
      while (i < length) {
        Node current = sortedNodes.get(i);
        if (current.getWeight() == 0) {
          _straws.put(current, 0L);
          i++;
          continue;
        }
        _straws.put(current, (long)(straw*0x10000));
        i++;
        if (i == length) {
          break;
        }

        current = sortedNodes.get(i);
        Node previous = sortedNodes.get(i-1);
        if (current.getWeight() == previous.getWeight()) {
          continue;
        }
        wbelow += (float)(previous.getWeight() - lastw)*numLeft;
        for (int j = i; j < length; j++) {
          if (sortedNodes.get(j).getWeight() == current.getWeight()) {
            numLeft--;
          } else {
            break;
          }
        }
        float wnext = (float)(numLeft * (current.getWeight() - previous.getWeight()));
        float pbelow = wbelow/(wbelow + wnext);
        straw *= Math.pow(1.0/pbelow, 1.0/numLeft);
        lastw = previous.getWeight();
      }
    }
    _hashFunction = new JenkinsHash();
  }

  /**
   * Returns a new list that's sorted in the reverse order of the weight.
   */
  private List<Node> sortNodes(List<Node> nodes) {
    List<Node> ret = new ArrayList<Node>(nodes);
    sortNodesInPlace(ret);
    return ret;
  }

  /**
   * Sorts the list in place in the reverse order of the weight.
   */
  private void sortNodesInPlace(List<Node> nodes) {
    Collections.sort(nodes, new Comparator<Node>() {
      public int compare(Node n1, Node n2) {
        if (n2.getWeight() == n1.getWeight()) {
          return 0;
        }
        return (n2.getWeight() - n1.getWeight() > 0) ? 1 : -1;
        // sort by weight only in the reverse order
      }
    });
  }

  public Node select(long input, long round) {
    Node selected = null;
    long hiScore = -1;
    for (Map.Entry<Node,Long> e: _straws.entrySet()) {
      Node child = e.getKey();
      long straw = e.getValue();
      long score = weightedScore(child, straw, input, round);
      if (score > hiScore) {
        selected = child;
        hiScore = score;
      }
    }
    if (selected == null) {
      throw new IllegalStateException();
    }
    return selected;
  }

  private long weightedScore(Node child, long straw, long input, long round) {
    long hash = _hashFunction.hash(input, child.getId(), round);
    hash = hash&0xffff;
    long weightedScore = hash*straw;
    return weightedScore;
  }
}
