package com.linkedin.helix;

import java.util.TimerTask;

public abstract class HelixTimerTask extends TimerTask
{
  public abstract void start();
  public abstract void stop();
}
