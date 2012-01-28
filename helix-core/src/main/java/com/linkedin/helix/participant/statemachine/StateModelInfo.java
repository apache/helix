package com.linkedin.helix.participant.statemachine;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface StateModelInfo
{
  String[] states();

  String initialState();

}
