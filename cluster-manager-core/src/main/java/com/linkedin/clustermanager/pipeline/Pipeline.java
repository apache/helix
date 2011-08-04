package com.linkedin.clustermanager.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.controller.stages.ClusterEvent;

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

  public void handle(ClusterEvent event)
  {
    if (_stages == null)
    {
      return;
    }
    for (Stage stage : _stages)
    {
      stage.preProcess();
      try
      {
        stage.process(event);
      } catch (Exception e)
      {
        logger.error("Exception while executing stage:" + stage
            + ". Pipeline will not continue to next stage", e);
        break;
      } finally
      {
        stage.postProcess();
      }
    }
  }

  public void finish()
  {

  }

}
