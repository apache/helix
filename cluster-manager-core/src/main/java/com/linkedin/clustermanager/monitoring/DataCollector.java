package com.linkedin.clustermanager.monitoring;

public abstract class DataCollector
{
   abstract void notifyDataSample(Object dataSample);
}
