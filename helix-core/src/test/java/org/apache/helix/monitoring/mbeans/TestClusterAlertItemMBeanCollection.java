package org.apache.helix.monitoring.mbeans;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.helix.ZNRecord;
import org.apache.helix.alerts.AlertValueAndStatus;
import org.apache.helix.alerts.Tuple;
import org.apache.helix.healthcheck.TestWildcardAlert.TestClusterMBeanObserver;
import org.apache.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestClusterAlertItemMBeanCollection
{
  private static final Logger _logger = Logger.getLogger(TestClusterAlertItemMBeanCollection.class);

  @Test
  public void TestAlertReportingHistory() throws InstanceNotFoundException, MalformedObjectNameException, NullPointerException, IOException, IntrospectionException, AttributeNotFoundException, ReflectionException, MBeanException
  {
    ClusterAlertMBeanCollection beanCollection = new ClusterAlertMBeanCollection();
    
    String clusterName = "TestCluster";
    String originAlert1 = "EXP(decay(1.0)(esv4-app7*.RestQueryStats@DBName=BizProfile.MinServerLatency))CMP(GREATER)CON(10)";
    Map<String, AlertValueAndStatus> alertResultMap1 = new HashMap<String, AlertValueAndStatus>();
    int nAlerts1 = 5;
    
    String originAlert2 = "EXP(decay(1.0)(esv4-app9*.RestQueryStats@DBName=BizProfile.MaxServerLatency))CMP(GREATER)CON(10)";
    Map<String, AlertValueAndStatus> alertResultMap2 = new HashMap<String, AlertValueAndStatus>();
    int nAlerts2 = 3;
    
    TestClusterMBeanObserver jmxMBeanObserver = new TestClusterMBeanObserver(ClusterAlertMBeanCollection.DOMAIN_ALERT);
    
    for(int i = 0; i < nAlerts1; i++)
    {
      String alertName = "esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MinServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 0; i < nAlerts2; i++)
    {
      String alertName = "esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MaxServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    beanCollection.setAlerts(originAlert1, alertResultMap1, clusterName);
    beanCollection.setAlerts(originAlert2, alertResultMap2, clusterName);
    
    beanCollection.refreshAlertDelta(clusterName);
    String summaryKey = ClusterAlertMBeanCollection.ALERT_SUMMARY + "_" + clusterName;
    jmxMBeanObserver.refresh();
    
    // Get the history list
    String beanName = "HelixAlerts:alert=" + summaryKey;
    Map<String, Object> beanValueMap = jmxMBeanObserver._beanValueMap.get(beanName);
    String history1 = (String) (beanValueMap.get("AlertFiredHistory"));
    
    StringReader sr = new StringReader(history1);
    ObjectMapper mapper = new ObjectMapper();

    // check the history
   
    Map<String, String> delta = beanCollection.getRecentAlertDelta();
    Assert.assertEquals(delta.size(), nAlerts1 + nAlerts2);
    for(int i = 0; i < nAlerts1; i++)
    {
      String alertBeanName = "(esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MinServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("ON"));
    }
    
    for(int i = 0; i < nAlerts2; i++)
    {
      String alertBeanName = "(esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MaxServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("ON"));
    }
    
    alertResultMap1 = new HashMap<String, AlertValueAndStatus>();
    for(int i = 0; i < 3; i++)
    {
      String alertName = "esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MinServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 3; i < 5; i++)
    {
      String alertName = "esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MinServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , false);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 7; i < 9; i++)
    {
      String alertName = "esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MinServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 0; i < 2; i++)
    {
      String alertName = "esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MaxServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , false);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 2; i < 3; i++)
    {
      String alertName = "esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MaxServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    for(int i = 7; i < 9; i++)
    {
      String alertName = "esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MaxServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    beanCollection.setAlerts(originAlert1, alertResultMap1, clusterName);
    beanCollection.refreshAlertDelta(clusterName);
    jmxMBeanObserver.refresh();

    beanValueMap = jmxMBeanObserver._beanValueMap.get(beanName);
    history1 = (String) (beanValueMap.get("AlertFiredHistory"));
    
    sr = new StringReader(history1);
    mapper = new ObjectMapper();

    // check the history
    delta = beanCollection.getRecentAlertDelta();
    Assert.assertEquals(delta.size(),  8);
    for(int i = 3; i < 5; i++)
    {
      String alertBeanName = "(esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MinServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("OFF"));
    }
    for(int i = 7; i < 9; i++)
    {
      String alertBeanName = "(esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MinServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("ON"));
    }
    
    for(int i = 0; i < 2; i++)
    {
      String alertBeanName = "(esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MaxServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("OFF"));
    }
    for(int i = 7; i < 9; i++)
    {
      String alertBeanName = "(esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName#BizProfile.MaxServerLatency)GREATER(10)";
      Assert.assertTrue(delta.get(alertBeanName).equals("ON"));
    }
  }
  @Test
  public void TestAlertRefresh() throws InstanceNotFoundException, MalformedObjectNameException, NullPointerException, IOException, IntrospectionException, AttributeNotFoundException, ReflectionException, MBeanException, InterruptedException
  {
    ClusterAlertMBeanCollection beanCollection = new ClusterAlertMBeanCollection();
    
    String clusterName = "TestCluster";
    String originAlert1 = "EXP(decay(1.0)(esv4-app7*.RestQueryStats@DBName=BizProfile.MinServerLatency))CMP(GREATER)CON(10)";
    Map<String, AlertValueAndStatus> alertResultMap1 = new HashMap<String, AlertValueAndStatus>();
    int nAlerts1 = 5;
    
    String originAlert2 = "EXP(decay(1.0)(esv4-app9*.RestQueryStats@DBName=BizProfile.MaxServerLatency))CMP(GREATER)CON(10)";
    Map<String, AlertValueAndStatus> alertResultMap2 = new HashMap<String, AlertValueAndStatus>();
    int nAlerts2 = 3;
    
    TestClusterMBeanObserver jmxMBeanObserver = new TestClusterMBeanObserver(ClusterAlertMBeanCollection.DOMAIN_ALERT);
    
    for(int i = 0; i < nAlerts1; i++)
    {
      String alertName = "esv4-app7" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MinServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap1.put(alertName, valueAndStatus);
    }
    
    for(int i = 0; i < nAlerts2; i++)
    {
      String alertName = "esv4-app9" + i + ".stg.linkedin.com_12918.RestQueryStats@DBName=BizProfile.MaxServerLatency";
      Tuple<String> value = new Tuple<String>();
      value.add("22" + i);
      AlertValueAndStatus valueAndStatus = new AlertValueAndStatus(value , true);
      alertResultMap2.put(alertName, valueAndStatus);
    }
    
    beanCollection.setAlerts(originAlert1, alertResultMap1, clusterName);
    beanCollection.setAlerts(originAlert2, alertResultMap2, clusterName);
    
    beanCollection.refreshAlertDelta(clusterName);
    String summaryKey = ClusterAlertMBeanCollection.ALERT_SUMMARY + "_" + clusterName;
    jmxMBeanObserver.refresh();
    
    Assert.assertEquals(jmxMBeanObserver._beanValueMap.size(), nAlerts2 + nAlerts1 + 1);
    
    Thread.sleep(300);
    
    beanCollection.setAlerts(originAlert1, alertResultMap1, clusterName);
    beanCollection.checkMBeanFreshness(200);
    
    for(int i = 0; i < 10; i++)
    {
      Thread.sleep(500);
      
      jmxMBeanObserver.refresh();
      
      if(jmxMBeanObserver._beanValueMap.size() == nAlerts1 + 1)
      {
        break;
      }
    }
    Assert.assertEquals(jmxMBeanObserver._beanValueMap.size(), nAlerts1 + 1);
  }
}
