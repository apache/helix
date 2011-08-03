package com.linkedin.clustermanager.controller.stages;

public interface Stage
{

  void init(StageContext context);
  
  void preProcess();
  
  public void process(ClusterEvent event) throws Exception;
  
  void postProcess();
  
  void release();
}
