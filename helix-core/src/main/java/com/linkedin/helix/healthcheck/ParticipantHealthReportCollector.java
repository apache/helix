package com.linkedin.helix.healthcheck;

import com.linkedin.helix.ZNRecord;

public interface ParticipantHealthReportCollector
{
  public abstract void addHealthReportProvider(HealthReportProvider provider);

  public abstract void removeHealthReportProvider(HealthReportProvider provider);

  public abstract void reportHealthReportMessage(ZNRecord healthReport);

  public abstract void transmitHealthReports();
}