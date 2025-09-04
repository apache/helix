package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test to verify that Straw2Selector properly handles empty nodes without throwing NPE.
 * This reproduces the bug where Straw2Selector returned null causing NPE in CRUSHPlacementAlgorithm.
 */
public class TestStraw2SelectorNPEFix {

  private Node createEmptyNode(String name, String type, long id) {
    Node node = new Node();
    node.setName(name);
    node.setType(type);
    node.setId(id);
    return node;
  }

  @DataProvider(name = "inputValues")
  public Object[][] getInputValues() {
    return new Object[][] {
        {12345L, 1L},
        {-1219343163L, 1L},
        {0L, 1L},
        {Long.MAX_VALUE, 1L},
        {Long.MIN_VALUE, 1L},
        {999L, 0L},
        {999L, 100L},
        {999L, Long.MAX_VALUE}
    };
  }

  @Test(dataProvider = "inputValues", expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "No node selected.*")
  public void testStraw2SelectorEmptyNodeThrowsException(long input, long round) {
    Node emptyNode = createEmptyNode("empty_mz", "mz", 123L);
    new Straw2Selector(emptyNode).select(input, round);
  }

  @DataProvider(name = "selectors")
  public Object[][] getSelectors() {
    Node emptyNode = createEmptyNode("empty", "test", 1L);
    return new Object[][] {
        { new StrawSelector(emptyNode) },
        { new Straw2Selector(emptyNode) }
    };
  }

  @Test(dataProvider = "selectors", expectedExceptions = IllegalStateException.class)
  public void testSelectorsConsistency(Selector selector) {
    selector.select(123L, 1L);
  }
}