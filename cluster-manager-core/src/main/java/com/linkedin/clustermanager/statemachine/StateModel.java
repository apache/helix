package com.linkedin.clustermanager.statemachine;

public abstract class StateModel
{
  // TODO Get default state from implementation or from state model definition
  private String _currentState = "OFFLINE";

  public String getCurrentState()
  {
    return _currentState;
  }

  // @transition(from='from', to='to')
  public void defaultTransitionHandler()
  {

  }

  public boolean updateState(String newState)
  {
    _currentState = newState;
    return true;
  }
}
