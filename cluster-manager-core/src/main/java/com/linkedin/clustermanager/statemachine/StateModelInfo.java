package com.linkedin.clustermanager.statemachine;

public @interface StateModelInfo
{
    String[] states();
    
    String initialState();
    
}
