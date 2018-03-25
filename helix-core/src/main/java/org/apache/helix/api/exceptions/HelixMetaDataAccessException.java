package org.apache.helix.api.exceptions;

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

import org.apache.helix.HelixException;

/**
 * Class for an exception thrown by Helix due to Helix's failure to read or write some metadata from zookeeper.
 */
public class HelixMetaDataAccessException extends HelixException {
  private static final long serialVersionUID = 6558251214364526258L;

  public HelixMetaDataAccessException(String message) {
    super(message);
  }

  public HelixMetaDataAccessException(Throwable cause) {
    super(cause);
  }

  public HelixMetaDataAccessException(String message, Throwable cause) {
    super(message, cause);
  }
}
