package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;
import com.linkedin.clustermanager.pipeline.AbstractBaseStage;
import com.linkedin.clustermanager.pipeline.StageException;

public class ExternalViewComputeStage extends AbstractBaseStage
{
	private static Logger log = Logger.getLogger(ExternalViewComputeStage.class);

	@Override
	public void process(ClusterEvent event) throws Exception
	{
		ClusterManager manager = event.getAttribute("clustermanager");
		if (manager == null)
		{
			throw new StageException("ClusterManager attribute value is null");
		}
		log.info("START ExternalViewComputeStage.process()");
		ClusterDataAccessor dataAccessor = manager.getDataAccessor();
		Map<String, ResourceGroup> resourceGroupMap = event
		    .getAttribute(AttributeName.RESOURCE_GROUPS.toString());
		if (resourceGroupMap == null)
		{
		  throw new StageException("ResourceGroupMap attribute value is null");
		}
		
		CurrentStateOutput currentStateOutput = event
		    .getAttribute(AttributeName.CURRENT_STATE.toString());
		for (String resourceGroupName : resourceGroupMap.keySet())
		{
			ZNRecord viewRecord = new ZNRecord();
			viewRecord.setId(resourceGroupName);
			ExternalView view = new ExternalView(viewRecord);
			ResourceGroup resourceGroup = resourceGroupMap.get(resourceGroupName);
			for (ResourceKey resource : resourceGroup.getResourceKeys())
			{
				Map<String, String> currentStateMap = currentStateOutput
				    .getCurrentStateMap(resourceGroupName, resource);
				if (currentStateMap != null && currentStateMap.size() > 0)
				{
					view.setStateMap(resource.getResourceKeyName(), currentStateMap);
				}
			}
			dataAccessor.setClusterProperty(ClusterPropertyType.EXTERNALVIEW,
			    resourceGroupName, view.getRecord());
		}
		log.info("START ExternalViewComputeStage.process()");
	}

}
