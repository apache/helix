package com.linkedin.helix.controller.pipeline;

import com.linkedin.helix.controller.stages.ClusterEvent;

public interface Stage
{

  void init(StageContext context);
  
  void preProcess();
  
  public void process(ClusterEvent event) throws Exception;
  
  void postProcess();
  
  void release();
}
