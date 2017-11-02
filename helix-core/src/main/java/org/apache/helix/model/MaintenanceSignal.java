package org.apache.helix.model;

import org.apache.helix.ZNRecord;

public class MaintenanceSignal extends PauseSignal {
  public MaintenanceSignal(String id) {
    super(id);
  }

  public MaintenanceSignal(ZNRecord record) {
    super(record);
  }
}
