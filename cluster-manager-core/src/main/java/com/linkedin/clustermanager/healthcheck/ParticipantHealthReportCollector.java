package com.linkedin.clustermanager.healthcheck;

import com.linkedin.clustermanager.ZNRecord;

public interface ParticipantHealthReportCollector
{
  public abstract void addHealthReportProvider(
      HealthReportProvider provider);

  public abstract void removeHealthReportProvider(
      HealthReportProvider provider);

  public abstract void reportHealthReportMessage(ZNRecord healthReport);

}