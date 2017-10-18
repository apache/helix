package org.apache.helix.rest.server.auditlog.auditloggers;

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

import com.sun.research.ws.wadl.HTTPMethods;
import org.apache.helix.rest.server.auditlog.AuditLog;
import org.apache.helix.rest.server.auditlog.AuditLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default AuditLogger implementation that log the AuditLog into local log file.
 */
public class FileBasedAuditLogger implements AuditLogger {
  private static Logger _logger = LoggerFactory.getLogger(FileBasedAuditLogger.class.getName());

  private boolean _logReadOperations;

  public FileBasedAuditLogger() {
    this(false);
  }

  public FileBasedAuditLogger(boolean logReadOperations) {
    _logReadOperations = logReadOperations;
  }

  @Override
  public void write(AuditLog auditLog) {
    if (_logReadOperations) {
      _logger.info(auditLog.toString());
    } else if (auditLog.getHttpMethod() == HTTPMethods.PUT.name()
        || auditLog.getHttpMethod() == HTTPMethods.POST.name()
        || auditLog.getHttpMethod() == HTTPMethods.DELETE.name()) {
      _logger.info(auditLog.toString());
    }
  }
}
