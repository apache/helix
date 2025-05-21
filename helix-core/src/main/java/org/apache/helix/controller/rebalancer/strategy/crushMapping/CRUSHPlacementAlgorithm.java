package org.apache.helix.controller.rebalancer.strategy.crushMapping;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.util.JenkinsHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The transcription of the CRUSH placement algorithm from the Weil paper. This is a fairly simple
 * adaptation, but a couple of important changes have been made to work with the crunch mapping.
 */
public class CRUSHPlacementAlgorithm {
  /**
   * In case the select() method fails to select after looping back to the origin of selection after
   * so many tries, we stop the search. This constant denotes the maximum number of retries after
   * looping back to the origin. It is expected that in most cases the selection will either succeed
   * with a small number of tries, or it will never succeed. So a reasonably large number to
   * distinguish these two cases should be sufficient.
   */
  private static final int MAX_LOOPBACK_COUNT = 50;
  private static final Logger logger = LoggerFactory.getLogger(CRUSHPlacementAlgorithm.class);

  private final boolean keepOffset;
  private final Map<Long,Integer> roundOffset;
  private final Selector.StrawBucket strawBucket;

  /**
   * Creates the crush placement object.
   */
  public CRUSHPlacementAlgorithm() {
    this(false);
  }

  /**
   * Creates the crush placement algorithm with the indication whether the round offset should be
   * kept for the duration of this object for successive selection of the same input.
   */
  public CRUSHPlacementAlgorithm(boolean keepOffset) {
    this(keepOffset, Selector.StrawBucket.STRAW);
  }

  public CRUSHPlacementAlgorithm(Selector.StrawBucket strawBucket) {
    this(false, strawBucket);
  }

  public CRUSHPlacementAlgorithm(boolean keepOffset, Selector.StrawBucket strawBucket) {
    this.keepOffset = keepOffset;
    roundOffset = keepOffset ? new HashMap<Long,Integer>() : null;
    this.strawBucket = strawBucket;
  }

  /**
   * Returns a list of (count) nodes of the desired type. If the count is more than the number of
   * available nodes, an exception is thrown. Note that it is possible for this method to return a
   * list whose size is smaller than the requested size (count) if it is unable to select all the
   * nodes for any reason. Callers should check the size of the returned list and take action if
   * needed.
   */
  public List<Node> select(Node parent, long input, int count, String type) {
    return select(parent, input, count, type, Predicates.<Node>alwaysTrue());
  }

  public List<Node> select(Node parent, long input, int count, String type,
      Predicate<Node> nodePredicate) {
    int childCount = parent.getChildrenCount(type);
    if (childCount < count) {
      logger.error(count + " nodes of type " + type +
          " were requested but the tree has only " + childCount + " nodes!");
    }

    List<Node> selected = new ArrayList<Node>(count);
    // use the index stored in the map
    Integer offset;
    if (keepOffset) {
      offset = roundOffset.get(input);
      if (offset == null) {
        offset = 0;
        roundOffset.put(input, offset);
      }
    } else {
      offset = 0;
    }

    int rPrime = 0;
    for (int r = 1; r <= count; r++) {
      int failure = 0;
      // number of times we had to loop back to the origin
      int loopbackCount = 0;
      boolean escape = false;
      boolean retryOrigin;
      Node out = null;
      do {
        retryOrigin = false; // initialize at the outset
        Node in = parent;
        Set<Node> rejected = new HashSet<Node>();
        boolean retryNode;
        do {
          retryNode = false; // initialize at the outset
          rPrime = r + offset + failure;
          logger.trace("{}.select({}, {})", new Object[] {in, input, rPrime});
          Selector selector = SelectorFactory.createSelector(in, strawBucket);
          out = selector.select(input, rPrime);
          if (!out.getType().equalsIgnoreCase(type)) {
            logger.trace("selected output {} for data {} didn't match the type {}: walking down " +
                "the hierarchy...", new Object[] {out, input, type});
            in = out; // walk down the hierarchy
            retryNode = true; // stay within the node and walk down the tree
          } else { // type matches
            boolean predicateRejected = !nodePredicate.apply(out);
            if (selected.contains(out) || predicateRejected) {
              if (predicateRejected) {
                logger.trace("{} was rejected by the node predicate for data {}: rejecting and " +
                    "increasing rPrime", out, input);
                rejected.add(out);
              } else { // already selected
                logger.trace("{} was already selected for data {}: rejecting and increasing rPrime",
                    out, input);
              }

              // we need to see if we have selected all possible nodes from this parent, in which
              // case we should loop back to the origin and start over
              if (allChildNodesEliminated(in, selected, rejected)) {
                logger.trace("all child nodes of {} have been eliminated", in);
                if (loopbackCount == MAX_LOOPBACK_COUNT) {
                  // we looped back the maximum times we specified; we give up search, and exit
                  escape = true;
                  break;
                }
                loopbackCount++;
                logger.trace("looping back to the original parent node ({})", parent);
                retryOrigin = true;
              } else {
                retryNode = true; // go back and reselect on the same parent
              }
              failure++;
            } else if (nodeIsOut(out)) {
              logger.trace("{} is marked as out (failed or over the maximum assignment) for data " +
                  "{}! looping back to the original parent node", out, input);
              failure++;
              if (loopbackCount == MAX_LOOPBACK_COUNT) {
                // we looped back the maximum times we specified; we give up search, and exit
                escape = true;
                break;
              }
              loopbackCount++;
              // re-selection on the same parent is detrimental in case of node failure: loop back
              // to the origin
              retryOrigin = true;
            } else {
              // we got a successful selection
              break;
            }
          }
        } while (retryNode);
      } while (retryOrigin);

      if (escape) {
        // cannot find a node under this parent; return a smaller set than was intended
        logger.debug("we could not select a node for data {} under parent {}; a smaller data set " +
            "than is requested will be returned", input, parent);
        continue;
      }

      logger.trace("{} was selected for data {}", out, input);
      selected.add(out);
    }
    if (keepOffset) {
      roundOffset.put(input, rPrime);
    }
    return selected;
  }


  private boolean nodeIsOut(Node node) {
    if (node.getWeight() == 0) {
      return true;
    }
    if (node.isLeaf() && node.isFailed()) {
      return true;
    }
    return false;
  }

  /**
   * Examines the immediate child nodes of the given parent node, and sees if all of the children
   * that can be selected (i.e. not failed) are already selected. This is used to determine whether
   * this parent node should no longer be used in the selection.
   */
  private boolean allChildNodesEliminated(Node parent, List<Node> selected, Set<Node> rejected) {
    List<Node> children = parent.getChildren();
    if (children != null) {
      for (Node child: children) {
        if (!nodeIsOut(child) && !selected.contains(child) && !rejected.contains(child)) {
          return false;
        }
      }
    }
    return true;
  }
}
