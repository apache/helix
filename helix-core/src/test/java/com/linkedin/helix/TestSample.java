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
package com.linkedin.helix;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.manager.zk.ZKDataAccessor;

/**
 *
 * @author kgopalak
 *
 */

public class TestSample
{

  @Test ()
  public final void testCallbackHandler()
  {
    ZKDataAccessor client = null;
    String path = null;
    Object listener = null;
    EventType[] eventTypes = null;

  }

  @BeforeMethod ()
  public void asd()
  {
    System.out.println("In Set up");
  }

  @Test ()
  public void testB()
  {
    System.out.println("In method testB");

  }

  @Test ()
  public void testA()
  {
    System.out.println("In method testA");

  }

  @AfterMethod ()
  public void sfds()
  {
    System.out.println("In tear down");
  }
}
