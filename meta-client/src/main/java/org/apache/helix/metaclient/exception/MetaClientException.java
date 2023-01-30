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
    /** Everything is OK. */
    OK {
      @Override
      public MetaClientException createMetaClientExceptionFromReturnCode(ReturnCode returnCode) {
        return null;
      }

      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "OK.";
      }
    },

    /** Indicates a system and server-side errors not defined by following codes.
     * It also indicate a range. Any value larger than this and less than DBUSERERROR
     * indicating error from server side.*/
    DB_SYSTEM_ERROR {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "System and server-side errors.";
      }
    },

    /** Connection to the server has been lost. */
    CONNECTION_LOSS {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Connection to the server has been lost.";
      }
    },

    /** Operation is unimplemented. */
    UNIMPLEMENTED {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Operation is unimplemented.";
      }
    },

    /** Operation timeout. */
    OPERATION_TIMEOUT {
      @Override
      public MetaClientException createMetaClientExceptionFromReturnCode(ReturnCode returnCode) {
        return new MetaClientTimeoutException();
      }

      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Operation timeout.";
      }
    },

    /** Either a runtime or data inconsistency was found. */
    CONSISTENCY_ERROR {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Inconsistency was found.";
      }
    },

    /** Session is moved or expired or non-exist. */
    SESSION_ERROR {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Session is moved or expired or non-exist.";
      }
    },

    /** Indicates a system and DB client or a usage errors not defined by following codes.
     * It also indicate a range. Any value larger than this indicating error from
     * client or caused by wrong usage.
     */
    DB_USER_ERROR {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Client or usage error.";
      }
    },

    /** The listener does not exists. */
    INVALID_LISTENER {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Listener does not exists.";
      }
    },

    /** Authentication failed. */
    AUTH_FAILED {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "authentication failed";
      }
    },

    /** Invalid arguments. */
    INVALID_ARGUMENTS {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Invalid arguments";
      }
    },

    /** Version conflict. Return when caller tries to edit an entry with a specific version but
     * the actual version of the entry on server is different. */
    BAD_VERSION {
      @Override
      public MetaClientException createMetaClientExceptionFromReturnCode(ReturnCode returnCode) {
        return new MetaClientBadVersionException();
      }

      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return " Version conflict.";
      }
    },

    /** Entry already exists. Return when try to create a duplicated entry. */
    ENTRY_EXISTS {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Entry already exists.";
      }
    },

    /** The client is not Authenticated. */
    NO_AUTH {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Not Authenticated.";
      }
    },

    /** Entry does not exist. */
    NO_SUCH_ENTRY {
      @Override
      public MetaClientException createMetaClientExceptionFromReturnCode(ReturnCode returnCode) {
        return new MetaClientNoNodeException();
      }

      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "Entry does not exist.";
      }
    },

    /**The entry has sub entries. Return when operation can only be down at entry with no
     * sub entries. (i.e. unrecursively delete an entry . )*/
    NOT_LEAF_ENTRY {
      @Override
      public String getCodeMessage(ReturnCode returnCode) {
        return "The entry has sub entries.";
      }
    };

    public MetaClientException createMetaClientExceptionFromReturnCode(ReturnCode returnCode) {
      // TODO: add more code translation when new exception class is created.
      return new MetaClientException();
    }

    public abstract String getCodeMessage(ReturnCode returnCode);
  }

}
