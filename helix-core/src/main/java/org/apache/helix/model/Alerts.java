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
package org.apache.helix.model;

import java.util.Map;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;


public class Alerts extends HelixProperty
{

  // private final ZNRecord _record;

  public final static String nodeName = "Alerts";

  public enum AlertsProperty
  {
    SESSION_ID, FIELDS
  }

  // private final ZNRecord _record;

  public Alerts(String id)
  {
    super(id);
  }

  public Alerts(ZNRecord record)
  {
    // _record = record;
    super(record);

  }

  /*
   * public Alerts(ZNRecord record, Stat stat) { super(record, stat); }
   */

  public void setSessionId(String sessionId)
  {
    _record.setSimpleField(AlertsProperty.SESSION_ID.toString(), sessionId);
  }

  public String getSessionId()
  {
    return _record.getSimpleField(AlertsProperty.SESSION_ID.toString());
  }

  public String getInstanceName()
  {
    return _record.getId();
  }

  /*
   * public String getVersion() { return
   * _record.getSimpleField(AlertsProperty.CLUSTER_MANAGER_VERSION.toString());
   * }
   */

  public Map<String, Map<String, String>> getMapFields()
  {
    return _record.getMapFields();
  }

  public Map<String, String> getStatFields(String statName)
  {
    return _record.getMapField(statName);
  }

  @Override
  public boolean isValid()
  {
    // TODO Auto-generated method stub
    return true;
  }
}
