package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.util.JenkinsHash;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


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

  @Test
  public void testStraw2SelectorZeroHashCase() throws NoSuchFieldException, IllegalAccessException {
    // validate that there is a tuple that return lowest 16 bits 0
    JenkinsHash hashFn = new JenkinsHash();
    Long hash = hashFn.hash(16, 24, 92);
    Assert.assertEquals(hash&0xffff, 0, "Expected low 16 bits to be zero");

    JenkinsHash spyHash = Mockito.spy(hashFn);

    // Create a node with child node
    Node parentNode = createEmptyNode("empty_mz", "mz", 123L);
    Node childNode = createEmptyNode("empty", "test", 24);
    parentNode.addChild(childNode);
    childNode.setWeight(1000); // default instance weight.
    Selector selector = new Straw2Selector(parentNode);
    java.lang.reflect.Field field =
        Straw2Selector.class.getDeclaredField("_hashFunction");
    field.setAccessible(true);
    field.set(selector, spyHash);

    Node selectedNode = selector.select(16, 92);
    verify(spyHash).hash(16, 24, 92);
    Assert.assertEquals(selectedNode, childNode,
        "The selector should still choose the only child node, "
            + "even if the hash function returns a value with low 16 bits equal to zero.");
  }

}