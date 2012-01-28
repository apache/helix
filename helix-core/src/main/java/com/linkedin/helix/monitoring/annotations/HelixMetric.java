package com.linkedin.helix.monitoring.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HelixMetric
{
  public String description();

}
