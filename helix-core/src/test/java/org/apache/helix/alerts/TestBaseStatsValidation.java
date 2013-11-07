package org.apache.helix.alerts;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixException;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test
public class TestBaseStatsValidation {

  @Test
  public void testParseSingletonExpression() {
    String[] actual = null;

    String statName = "window(5)(dbFoo.partition10.latency)";
    try {
      actual = ExpressionParser.getBaseStats(statName);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals(statName, actual[0]);
  }

  @Test
  public void testExtraParen() {
    String[] actual = null;

    String statName = "window(5)(dbFoo.partition10.latency)()";
    boolean caughtException = false;
    try {
      actual = ExpressionParser.getBaseStats(statName);
    } catch (HelixException e) {
      caughtException = true;
      // e.printStackTrace();
    }
    AssertJUnit.assertEquals(true, caughtException);
  }

  @Test
  public void testParseSingletonWildcardExpression() {
    String[] actual = null;

    String statName = "accumulate()(dbFoo.partition*.latency)";
    try {
      actual = ExpressionParser.getBaseStats(statName);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals(statName, actual[0]);
  }

  @Test
  public void testParsePairOfExpressions() {
    String[] actual = null;

    String expression = "accumulate()(dbFoo.partition10.latency, dbFoo.partition10.count)";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition10.latency)", actual[0]);
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition10.count)", actual[1]);
  }

  /*
   * SUM is not to be persisted, so pull out the pieces
   */
  @Test
  public void testSUMExpression() {
    String[] actual = null;

    String expression = "accumulate()(dbFoo.partition*.latency)|SUM";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition*.latency)", actual[0]);
  }

  @Test
  public void testSumPairExpression() {
    String[] actual = null;

    String expression = "window(5)(dbFoo.partition10.latency, dbFoo.partition11.latency)|SUM";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("window(5)(dbFoo.partition10.latency)", actual[0]);
    AssertJUnit.assertEquals("window(5)(dbFoo.partition11.latency)", actual[1]);
  }

  @Test
  public void testEachPairExpression() {
    String[] actual = null;

    String expression = "accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EACH";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition*.latency)", actual[0]);
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition*.count)", actual[1]);
  }

  @Test
  public void testAccumulateExpression() {
    String[] actual = null;

    String expression = "accumulate()(dbFoo.partition10.latency)|ACCUMULATE";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition10.latency)", actual[0]);
  }

  @Test
  public void testAccumulateEachExpression() {
    String[] actual = null;

    String expression = "window(5)(dbFoo.partition*.latency)|EACH|ACCUMULATE";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("window(5)(dbFoo.partition*.latency)", actual[0]);
  }

  @Test
  public void testAccumulateEachPairExpression() {
    String[] actual = null;

    String expression =
        "accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EACH|ACCUMULATE|DIVIDE";
    try {
      actual = ExpressionParser.getBaseStats(expression);
    } catch (HelixException e) {
      e.printStackTrace();
    }
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition*.latency)", actual[0]);
    AssertJUnit.assertEquals("accumulate()(dbFoo.partition*.count)", actual[1]);
  }

}
