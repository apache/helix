package org.apache.helix.ui.api;

import org.apache.helix.model.IdealState;

import java.util.List;

public class IdealStateSpec {
    private final int numPartitions;
    private final String replicas;
    private final String instanceGroupTag;
    private final int maxPartitionsPerInstance;
    private final String rebalanceMode;
    private final String rebalancerClassName;
    private final String stateModel;
    private final int bucketSize;
    private final int rebalanceTimerPeriod;
    private final boolean batchMessageMode;

    public IdealStateSpec(int numPartitions,
                          String replicas,
                          String instanceGroupTag,
                          int maxPartitionsPerInstance,
                          String rebalanceMode,
                          String rebalancerClassName,
                          String stateModel,
                          int bucketSize,
                          int rebalanceTimerPeriod,
                          boolean batchMessageMode) {
        this.numPartitions = numPartitions;
        this.replicas = replicas;
        this.instanceGroupTag = instanceGroupTag;
        this.maxPartitionsPerInstance = maxPartitionsPerInstance;
        this.rebalanceMode = rebalanceMode;
        this.rebalancerClassName = rebalancerClassName;
        this.stateModel = stateModel;
        this.bucketSize = bucketSize;
        this.rebalanceTimerPeriod = rebalanceTimerPeriod;
        this.batchMessageMode = batchMessageMode;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public String getReplicas() {
        return replicas;
    }

    public String getInstanceGroupTag() {
        return instanceGroupTag;
    }

    public int getMaxPartitionsPerInstance() {
        return maxPartitionsPerInstance;
    }

    public String getRebalanceMode() {
        return rebalanceMode;
    }

    public String getRebalancerClassName() {
        return rebalancerClassName;
    }

    public String getStateModel() {
        return stateModel;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public int getRebalanceTimerPeriod() {
        return rebalanceTimerPeriod;
    }

    public boolean isBatchMessageMode() {
        return batchMessageMode;
    }

    public static IdealStateSpec fromIdealState(IdealState idealState) {
        return new IdealStateSpec(
                idealState.getNumPartitions(),
                idealState.getReplicas(),
                idealState.getInstanceGroupTag(),
                idealState.getMaxPartitionsPerInstance(),
                idealState.getRebalanceMode().toString(),
                idealState.getRebalancerClassName(),
                idealState.getStateModelDefRef(),
                idealState.getBucketSize(),
                idealState.getRebalanceTimerPeriod(),
                idealState.getBatchMessageMode());
    }
}
