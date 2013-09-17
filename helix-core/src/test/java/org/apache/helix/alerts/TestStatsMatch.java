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

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test
public class TestStatsMatch {

  @Test
  public void testExactMatch() {

    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatExactMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testSingleWildcardMatch() {

    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testDoubleWildcardMatch() {

    String persistedStatName = "window(5)(db*.partition*.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testWildcardMatchNoWildcard() {

    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testWildcardMatchTooManyFields() {

    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
    String incomingStatName = "dbFoo.tableBar.partition10.latency";
    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testWildcardMatchTooFewFields() {

    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
    String incomingStatName = "dbFoo.latency";
    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName,
        incomingStatName));
  }

  @Test
  public void testBadWildcardRepeated() {

    String persistedStatName = "window(5)(dbFoo.partition**4.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    boolean match =
        ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName);

    AssertJUnit.assertFalse(match);
  }

  @Test
  public void testBadWildcardNotAtEnd() {

    String persistedStatName = "window(5)(dbFoo.*partition.latency)";
    String incomingStatName = "dbFoo.partition10.latency";
    boolean match =
        ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName);

    AssertJUnit.assertFalse(match);
  }
}
