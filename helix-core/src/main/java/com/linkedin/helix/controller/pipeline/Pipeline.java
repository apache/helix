package com.linkedin.helix.controller.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.controller.stages.ClusterEvent;

public class Pipeline
{
  private static final Logger logger = Logger.getLogger(Pipeline.class
      .getName());
  List<Stage> _stages;

  public Pipeline()
  {
    _stages = new ArrayList<Stage>();
  }

  public void addStage(Stage stage)
  {
    _stages.add(stage);
    StageContext context = null;
    stage.init(context);
  }

  public void handle(ClusterEvent event) throws Exception
  {
    if (_stages == null)
    {
      return;
    }
    for (Stage stage : _stages)
    {
      stage.preProcess();
      stage.process(event);
      stage.postProcess();
    }
  }

  public void finish()
  {

  }

}
