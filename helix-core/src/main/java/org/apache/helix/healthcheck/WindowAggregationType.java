/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.healthcheck;

import java.util.TimerTask;

import org.apache.log4j.Logger;

public class WindowAggregationType implements AggregationType
{

  private static final Logger logger = Logger
      .getLogger(WindowAggregationType.class);

  public final String WINDOW_DELIM = "#";

  public final static String TYPE_NAME = "window";

  int _windowSize = 1;

  public WindowAggregationType(int ws)
  {
    super();
    _windowSize = ws;
  }

  @Override
  public String getName()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(TYPE_NAME);
    sb.append(DELIM);
    sb.append(_windowSize);
    return sb.toString();
  }

  @Override
  public String merge(String incomingVal, String existingVal, long prevTimestamp)
  {
    String[] windowVals;
    if (existingVal == null)
    {
      return incomingVal;
    }
    else
    {
      windowVals = existingVal.split(WINDOW_DELIM);
      int currLength = windowVals.length;
      // window not full
      if (currLength < _windowSize)
      {
        return existingVal + WINDOW_DELIM + incomingVal;
      }
      // evict oldest
      else
      {
        int firstDelim = existingVal.indexOf(WINDOW_DELIM);
        return existingVal.substring(firstDelim + 1) + WINDOW_DELIM
            + incomingVal;
      }
    }
  }
}
