package com.linkedin.clustermanager.monitoring;

import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.monitoring.TestParticipantMonitor.ParticipantMonitorListener;
import com.linkedin.clustermanager.monitoring.mbeans.StateTransitionStatMonitor;
import com.linkedin.clustermanager.monitoring.mbeans.TransStatMonitorChangedListener;

public class TestParticipantMonitor
{
  class ParticipantMonitorListener implements TransStatMonitorChangedListener
  {
    HashSet<StateTransitionStatMonitor> _monitors = new HashSet<StateTransitionStatMonitor>();
    @Override
    public void onTransStatMonitorAdded(
        StateTransitionStatMonitor newStateTransitionStatMonitor)
    {
      // TODO Auto-generated method stub
      Assert.assertTrue(!_monitors.contains(newStateTransitionStatMonitor));
      _monitors.add(newStateTransitionStatMonitor);
    }
    
  }
  @Test(groups={ "unitTest" })
  public void TestReportData()
  {
    ParticipantMonitor monitor = ParticipantMonitor.getInstance();
    
    ParticipantMonitorListener monitorListener = new ParticipantMonitorListener();
    
    monitor.addTransStatMonitorChangedListener(monitorListener);
    
    StateTransitionContext cxt = new StateTransitionContext("cluster", "instance", "db_1","a-b");
    StateTransitionDataPoint data = new StateTransitionDataPoint(1000,1000,true);
    
    monitor.reportTransitionStat(cxt, data);
    Assert.assertTrue(monitorListener._monitors.size() == 1);
    
    data = new StateTransitionDataPoint(1000,500,true);
    monitor.reportTransitionStat(cxt, data);
    Assert.assertTrue(monitorListener._monitors.size() == 1);
    
    data = new StateTransitionDataPoint(1000,500,true);
    StateTransitionContext cxt2 = new StateTransitionContext("cluster", "instance", "db_2","a-b");
    monitor.reportTransitionStat(cxt2, data);
    Assert.assertTrue(monitorListener._monitors.size() == 2);
    
    Assert.assertFalse(cxt.equals(cxt2));
    Assert.assertFalse(cxt.equals(new Object()));
    Assert.assertTrue(cxt.equals(new StateTransitionContext("cluster", "instance", "db_1","a-b")));
    
    cxt2.getInstanceName();
    
    ParticipantMonitorListener monitorListener2 = new ParticipantMonitorListener();
    
    monitor.addTransStatMonitorChangedListener(monitorListener2);
    Assert.assertTrue(monitorListener2._monitors.size() == 2);
  }
}
