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
import org.apache.helix.alerts.AlertParser;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestAlertValidation {

  public final String EXP = AlertParser.EXPRESSION_NAME;
  public final String CMP = AlertParser.COMPARATOR_NAME;
  public final String CON = AlertParser.CONSTANT_NAME;

  @Test
  public void testSimple() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency)) " + CMP + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(caughtException);
  }

  @Test
  public void testSingleInSingleOut() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency)|EXPAND) " + CMP + "(GREATER) " + CON
            + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(caughtException);
  }

  @Test
  public void testDoubleInDoubleOut() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency, dbFoo.partition11.latency)|EXPAND) " + CMP
            + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test
  public void testTwoStageOps() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EXPAND|DIVIDE) "
            + CMP + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(caughtException);
  }

  @Test
  public void testTwoListsIntoOne() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency, dbFoo.partition11.count)|SUM) " + CMP
            + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(caughtException);
  }

  @Test
  public void testSumEach() {
    String alertName =
        EXP
            + "(accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EXPAND|SUMEACH|DIVIDE) "
            + CMP + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(caughtException);
  }

  @Test
  public void testNeedTwoTuplesGetOne() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition*.latency)|EXPAND|DIVIDE) " + CMP + "(GREATER) " + CON
            + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test
  public void testExtraPipe() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency)|) " + CMP + "(GREATER) " + CON + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(caughtException);
  }

  @Test
  public void testAlertUnknownOp() {
    String alertName =
        EXP + "(accumulate()(dbFoo.partition10.latency)|BADOP) " + CMP + "(GREATER) " + CON
            + "(10)";
    boolean caughtException = false;
    try {
      AlertParser.validateAlert(alertName);
    } catch (HelixException e) {
      caughtException = true;
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(caughtException);
  }
}
