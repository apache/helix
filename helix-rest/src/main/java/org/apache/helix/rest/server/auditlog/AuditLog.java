package org.apache.helix.rest.server.auditlog;

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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

public class AuditLog {
  private static Logger _logger = Logger.getLogger(AuditLog.class.getName());
  public static final String ATTRIBUTE_NAME = "AUDIT_LOG";

  private Date _startTime;
  private Date _completeTime;
  private Principal _principal;
  private String _clientIP;
  private String _clientHostPort;
  private String _requestPath;
  private String _httpMethod;
  private List<String> _requestHeaders;
  private String _requestEntity;
  private int _responseCode;
  private String _responseEntity;

  private List<Exception> _exceptions;
  private String _additionalInfo;

  public AuditLog(Date startTime, Date completeTime, Principal principal, String clientIP,
      String clientHostPort, String requestPath, String httpMethod, List<String> requestHeaders,
      String requestEntity, int responseCode, String responseEntity,
      String additionalInfo, List<Exception> exceptions) {
    _startTime = startTime;
    _completeTime = completeTime;
    _principal = principal;
    _clientIP = clientIP;
    _clientHostPort = clientHostPort;
    _requestPath = requestPath;
    _httpMethod = httpMethod;
    _requestHeaders = requestHeaders;
    _requestEntity = requestEntity;
    _responseCode = responseCode;
    _responseEntity = responseEntity;
    _additionalInfo = additionalInfo;
    _exceptions = exceptions;
  }

  @Override
  public String toString() {
    return "AuditLog{" +
        "_startTime=" + _startTime +
        ", _completeTime=" + _completeTime +
        ", _principal=" + _principal +
        ", _clientIP='" + _clientIP + '\'' +
        ", _clientHostPort='" + _clientHostPort + '\'' +
        ", _requestPath='" + _requestPath + '\'' +
        ", _httpMethod='" + _httpMethod + '\'' +
        ", _requestHeaders=" + _requestHeaders +
        ", _requestEntity='" + _requestEntity + '\'' +
        ", _responseCode=" + _responseCode +
        ", _responseEntity='" + _responseEntity + '\'' +
        ", _exceptions=" + _exceptions +
        ", _additionalInfo='" + _additionalInfo + '\'' +
        '}';
  }

  public Date getStartTime() {
    return _startTime;
  }

  public Date getCompleteTime() {
    return _completeTime;
  }

  public Principal getPrincipal() {
    return _principal;
  }

  public String getClientIP() {
    return _clientIP;
  }

  public String getClientHostPort() {
    return _clientHostPort;
  }

  public String getRequestPath() {
    return _requestPath;
  }

  public String getHttpMethod() {
    return _httpMethod;
  }

  public List<String> getRequestHeaders() {
    return _requestHeaders;
  }

  public String getRequestEntity() {
    return _requestEntity;
  }

  public int getResponseCode() {
    return _responseCode;
  }

  public String getResponseEntity() {
    return _responseEntity;
  }

  public List<Exception> getExceptions() {
    return _exceptions;
  }

  public String getAdditionalInfo() {
    return _additionalInfo;
  }

  public static class Builder {
    private Date _startTime;
    private Date _completeTime;
    private Principal _principal;
    private String _clientIP;
    private String _clientHostPort;
    private String _requestPath;
    private String _httpMethod;
    private List<String> _requestHeaders;
    private String _requestEntity;
    private int _responseCode;
    private String _responseEntity;

    private List<Exception> _exceptions;
    private String _additionalInfo;

    public Date getStartTime() {
      return _startTime;
    }

    public Builder startTime(Date startTime) {
      _startTime = startTime;
      return this;
    }

    public Date getCompleteTime() {
      return _completeTime;
    }

    public Builder completeTime(Date completeTime) {
      _completeTime = completeTime;
      return this;
    }

    public Principal getPrincipal() {
      return _principal;
    }

    public Builder principal(Principal principal) {
      _principal = principal;
      return this;
    }

    public String getClientIP() {
      return _clientIP;
    }

    public Builder clientIP(String clientIP) {
      _clientIP = clientIP;
      return this;
    }

    public String getClientHostPort() {
      return _clientHostPort;
    }

    public Builder clientHostPort(String clientHostPort) {
      _clientHostPort = clientHostPort;
      return this;
    }

    public String getRequestPath() {
      return _requestPath;
    }

    public Builder requestPath(String requestPath) {
      _requestPath = requestPath;
      return this;
    }

    public String getHttpMethod() {
      return _httpMethod;
    }

    public Builder httpMethod(String httpMethod) {
      _httpMethod = httpMethod;
      return this;
    }

    public String getRequestEntity() {
      return _requestEntity;
    }

    public Builder requestEntity(String requestEntity) {
      _requestEntity = requestEntity;
      return this;
    }

    public List<String> getRequestHeaders() {
      return _requestHeaders;
    }

    public Builder requestHeaders(List<String> requestHeaders) {
      _requestHeaders = requestHeaders;
      return this;
    }

    public int getResponseCode() {
      return _responseCode;
    }

    public Builder responseCode(int responseCode) {
      _responseCode = responseCode;
      return this;
    }

    public String getResponseEntity() {
      return _responseEntity;
    }

    public Builder responseEntity(String responseEntity) {
      _responseEntity = responseEntity;
      return this;
    }

    public List<Exception> getExceptions() {
      return _exceptions;
    }

    public Builder exceptions(List<Exception> exceptions) {
      _exceptions = exceptions;
      return this;
    }

    public Builder addException(Exception ex) {
      if (_exceptions == null) {
        _exceptions = new ArrayList<>();
      }
      _exceptions.add(ex);
      return this;
    }

    public String getAdditionalInfo() {
      return _additionalInfo;
    }

    public Builder additionalInfo(String additionalInfo) {
      _additionalInfo = additionalInfo;
      return this;
    }

    public AuditLog build() {
      return new AuditLog(_startTime, _completeTime, _principal, _clientIP, _clientHostPort,
          _requestPath, _httpMethod, _requestHeaders, _requestEntity, _responseCode,
          _responseEntity, _additionalInfo, _exceptions);
    }
  }

}
