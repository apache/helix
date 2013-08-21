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

public class AccumulateAggregator extends Aggregator {

  public AccumulateAggregator() {
    _numArgs = 0;
  }

  @Override
  public void merge(Tuple<String> currValTup, Tuple<String> newValTup, Tuple<String> currTimeTup,
      Tuple<String> newTimeTup, String... args) {

    double currVal = 0;
    double currTime = -1;
    double newVal;
    double newTime;
    double mergedVal;
    double mergedTime;

    if (currValTup == null || newValTup == null || currTimeTup == null || newTimeTup == null) {
      throw new HelixException("Tuples cannot be null");
    }

    // old tuples may be empty, indicating no value/time exist
    if (currValTup.size() > 0 && currTimeTup.size() > 0) {
      currVal = Double.parseDouble(currValTup.iterator().next());
      currTime = Double.parseDouble(currTimeTup.iterator().next());
    }
    newVal = Double.parseDouble(newValTup.iterator().next());
    newTime = Double.parseDouble(newTimeTup.iterator().next());

    if (newTime > currTime) { // if old doesn't exist, we end up here
      mergedVal = currVal + newVal; // if old doesn't exist, it has value "0"
      mergedTime = newTime;
    } else {
      mergedVal = currVal;
      mergedTime = currTime;
    }

    currValTup.clear();
    currValTup.add(Double.toString(mergedVal));
    currTimeTup.clear();
    currTimeTup.add(Double.toString(mergedTime));
  }

}
