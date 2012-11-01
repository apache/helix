package org.apache.helix.model;

public class Transition
{
  final private String _fromState;
  final private String _toState;

  public Transition(String fromState, String toState)
  {
    _fromState = fromState;
    _toState = toState;
  }

  @Override
  public String toString()
  {
    return _fromState + "-" + _toState;
  }

  @Override
  public int hashCode()
  {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object that)
  {
    if (that == null || !(that instanceof Transition))
    {
      return false;
    }
    return this.toString().equalsIgnoreCase(that.toString());
  }

  public String getFromState()
  {
    return _fromState;
  }

  public String getToState()
  {
    return _toState;
  }

}