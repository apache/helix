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
package org.apache.helix.store;

import org.apache.helix.store.PropertyStat;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestPropertyStat
{
  @Test (groups = {"unitTest"})
  public void testPropertyStat()
  {
    PropertyStat stat = new PropertyStat(0, 0);
    AssertJUnit.assertEquals(0, stat.getLastModifiedTime());
    AssertJUnit.assertEquals(0, stat.getVersion());
  }

}
