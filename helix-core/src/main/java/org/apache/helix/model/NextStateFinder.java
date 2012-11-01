package org.apache.helix.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NextStateFinder
{
  public static class Transition
  {
    final String _fromState;
    final String _toState;
    
    public Transition(String fromState, String toState)
    {
      _fromState = fromState;
      _toState = toState;
    }
  }

  // for convenient get path value, in which non-exist means MAX
  static int getPathVal(Map<String, Map<String, Integer>> path, String fromState, String toState)
  {
    if (!path.containsKey(fromState))
    {
      return Integer.MAX_VALUE;
    }

    if (!(path.get(fromState).containsKey(toState)))
    {
      return Integer.MAX_VALUE;
    }

    return path.get(fromState).get(toState);
  }

  static void setPathVal(Map<String, Map<String, Integer>> path, String fromState, String toState,
      int val)
  {
    if (!path.containsKey(fromState))
    {
      path.put(fromState, new HashMap<String, Integer>());
    }

    path.get(fromState).put(toState, val);
  }

  static void setNext(Map<String, Map<String, String>> next, String fromState, String toState, String nextState)
  {
    if (!next.containsKey(fromState))
    {
      next.put(fromState, new HashMap<String, String>());
    }

    next.get(fromState).put(toState, nextState);
    
  }
  
  /**
   * auxiliary method to get next state based on next map
   * 
   * @param next
   * @param fromState
   * @param toState
   * @return nextState or null if doesn't exist a path
   */
  public static String getNext(Map<String, Map<String, String>> next, String fromState, String toState)
  {
    if (!next.containsKey(fromState))
    {
      // no path
      return null;
    }
    
    return next.get(fromState).get(toState);
  }
  
  // debug
  static void printPath(List<String> states, Map<String, Map<String, String>> next)
  {
    for (String fromState : states)
    {
      for (String toState : states)
      {
        if (toState.equals(fromState))
        {
          // not print self-loop
          continue;
        }
        
        System.out.print(fromState);
        String nextState = getNext(next, fromState, toState);
        while (nextState != null && !nextState.equals(toState))
        {
          System.out.print("->" + nextState);
          nextState = getNext(next, nextState, toState);
        }
        
        if (nextState == null)
        {
          // no path between fromState -> toState
          System.out.println("->null" + toState + " (no path avaliable)");
        } else
        {
          System.out.println("->" + toState);
        }
      }
    }
  }
  
  /**
   * floyd-warshall
   * 
   * @param states
   * @param transitions
   * @return next map
   */
  public static Map<String, Map<String, String>> findNextState(List<String> states,
      List<Transition> transitions)
  {
    // path distance value
    Map<String, Map<String, Integer>> path = new HashMap<String, Map<String, Integer>>();

    // next state
    Map<String, Map<String, String>> next = new HashMap<String, Map<String, String>>();

    // init path and next
    for (String state : states)
    {
      setPathVal(path, state, state, 0);
      setNext(next, state, state, state);      
    }
    
    for (Transition transition : transitions)
    {
      String fromState = transition._fromState;
      String toState = transition._toState;
      setPathVal(path, fromState, toState, 1);
      setNext(next, fromState, toState, toState);
    }

    // iterate
    for (String intermediateState : states)
    {
      for (String fromState : states)
      {
        for (String toState : states)
        {
          int pathVal1 = getPathVal(path, fromState, intermediateState);
          int pathVal2 = getPathVal(path, intermediateState, toState);
          int pathValCur = getPathVal(path, fromState, toState);

          // should not overflow
          if (pathVal1 < Integer.MAX_VALUE && pathVal2 < Integer.MAX_VALUE
              && (pathVal1 + pathVal2) < pathValCur)
          {
            setPathVal(path, fromState, toState, pathVal1 + pathVal2);
            setNext(next, fromState, toState, intermediateState);
          }
        }
      }
    }
    
    return next;
  }
  
  public static void main(String[] args)
  {
    List<String> states = new ArrayList<String>();
    states.add("OFFLINE");
    states.add("SLAVE");
    states.add("MASTER");
    
    List<Transition> transitions = new ArrayList<Transition>();
    transitions.add(new Transition("OFFLINE", "SLAVE"));
    transitions.add(new Transition("SLAVE", "MASTER"));
    transitions.add(new Transition("MASTER", "SLAVE"));
    transitions.add(new Transition("SLAVE", "OFFLINE"));
    
    Map<String, Map<String, String>> next = findNextState(states, transitions);
    printPath(states, next);
  }
}