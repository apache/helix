package org.apache.helix.metaclient.exception;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class MetaClientException extends RuntimeException {
  public MetaClientException() {
    super();
  }

  public MetaClientException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaClientException(String message) {
    super(message);
  }

  public MetaClientException(Throwable cause) {
    super(cause);
  }

  public enum ReturnCode {
    /** Connection to the server has been lost. */
    CONNECTION_LOSS(-105, "Connection to the server has been lost.") ,

    /** Operation is unimplemented. */
    UNIMPLEMENTED(-104, "Operation is unimplemented."),

    /** Operation timeout. */
    OPERATION_TIMEOUT(-103, "Operation timeout.") {
      @Override
      public MetaClientException createMetaClientException() {
        return new MetaClientTimeoutException();
      }
    },

    /** Either a runtime or data inconsistency was found. */
    CONSISTENCY_ERROR(-102, "Inconsistency was found."),

    /** Session is moved or expired or non-exist. */
    SESSION_ERROR(-101, "Session is moved or expired or non-exist."),

    /** Indicates a system and server-side errors not defined by following codes.
     * It also indicate a range. Any value smaller or equal than this indicating error from
     * server side.
     */
    DB_SYSTEM_ERROR(-100, "System and server-side errors."),

    /** The listener does not exists. */
    INVALID_LISTENER(-9, "Listener does not exists."),

    /** Authentication failed. */
    AUTH_FAILED(-8, "authentication failed"),

    /** Invalid arguments. */
    INVALID_ARGUMENTS(-7, "Invalid arguments"),

    /** Version conflict. Return when caller tries to edit an entry with a specific version but
     * the actual version of the entry on server is different. */
    BAD_VERSION(-6, "Version conflict.") {
      @Override
      public MetaClientException createMetaClientException() {
        return new MetaClientBadVersionException();
      }
    },

    /** Entry already exists. Return when try to create a duplicated entry. */
    ENTRY_EXISTS(-5, "Entry already exists."),

    /** The client is not Authenticated. */
    NO_AUTH(-4, "Not Authenticated.") ,

    /** Entry does not exist. */
    NO_SUCH_ENTRY(-3, "Entry does not exist.") {
      @Override
      public MetaClientException createMetaClientException() {
        return new MetaClientNoNodeException();
      }
    },

    /**The entry has sub entries. Return when operation can only be down at entry with no
     * sub entries. (i.e. unrecursively delete an entry . )*/
    NOT_LEAF_ENTRY(-2, "The entry has sub entries."),

    /** Indicates a system and DB client or a usage errors not defined by following codes.
     * It also indicate a range. Any value smaller or equal than this and larger than
     * DB_SYSTEM_ERROR indicating error from client or caused by wrong usage.
     */
    DB_USER_ERROR(-1, "Client or usage error."),

    /** Everything is OK. */
    OK(0, "OK") {
      @Override
      public MetaClientException createMetaClientException() {
        return null;
      }
    };

    private final int _intValue;
    private final String _message;

    ReturnCode(int codeIntValue, String message) {
      _intValue = codeIntValue;
      _message = message;
    }

    public String getMessage() {
      return _message;
    }

    public int getIntValue() {
      return _intValue;
    }

    public MetaClientException createMetaClientException() {
      // TODO: add more code translation when new exception class is created.
      return new MetaClientException();
    }
  }

}
