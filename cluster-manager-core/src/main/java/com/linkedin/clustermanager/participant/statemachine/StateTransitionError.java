package com.linkedin.clustermanager.participant.statemachine;

public class StateTransitionError
{
  private final Exception _exception;
  private final ErrorCode _code;

  public StateTransitionError(ErrorCode code, Exception e)
  {
    this._code = code;
    _exception = e;
  }

  public Exception getException()
  {
    return _exception;
  }

  public ErrorCode getCode()
  {
    return _code;
  }

  enum ErrorCode
  {
    FRAMEWORK, INTERNAL
  }
}
