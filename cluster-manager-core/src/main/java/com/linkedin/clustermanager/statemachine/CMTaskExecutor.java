package com.linkedin.clustermanager.statemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.core.NotificationContext;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.ZNRecord;
import com.linkedin.clustermanager.util.StatusUpdateUtil;

public class CMTaskExecutor
{
    private static final int MAX_PARALLEL_TASKS = 1;
    private final ExecutorService _pool;
    protected final Map<String, Future<CMTaskResult>> _taskMap;
    private final Object _lock;
    StatusUpdateUtil _statusUpdateUtil;

    private static Logger logger = Logger.getLogger(CMTaskExecutor.class);

    public CMTaskExecutor()
    {
        _taskMap = new HashMap<String, Future<CMTaskResult>>();
        _lock = new Object();
        _statusUpdateUtil = new StatusUpdateUtil();
        _pool = Executors.newFixedThreadPool(MAX_PARALLEL_TASKS);
        startMonitorThread();

    }

    private void startMonitorThread()
    {
        // start a thread which monitors the completions of task
    }

    public void executeTask(Message message, StateModel stateModel,
            NotificationContext notificationContext)
    {
        synchronized (_lock)
        {
            try
            {
                logger.info("message.getMsgId() = " + message.getMsgId());
                
                _statusUpdateUtil.logInfo(
                    message, 
                    CMTaskExecutor.class, 
                    "Message handling task scheduled",
                    notificationContext.getManager().getDataAccessor()
                    );
                CMTaskHandler task = new CMTaskHandler(notificationContext,
                        message, stateModel);
                if (!_taskMap.containsKey(message.getMsgId()))
                {
                    Future<CMTaskResult> future = _pool.submit(task);
                    _taskMap.put(message.getMsgId(), future);
                }
                else
                {
                   _statusUpdateUtil.logWarning(
                      message, 
                      CMTaskExecutor.class, 
                      "Message handling task not found in _taskMap",
                      notificationContext.getManager().getDataAccessor()
                      );
                }
            }
            catch (Exception e)
            {
              String errorMessage = "Error while executing task"+ e;
              logger.error("Error while executing task." + message, e);
                
              _statusUpdateUtil.logError(
                  message, 
                  CMTaskExecutor.class, 
                  errorMessage,
                  notificationContext.getManager().getDataAccessor()
                  );
                // TODO add retry or update errors node
            }
        }
    }

    protected void reportCompletion(String msgId)
    {

    }
    public static void main(String[] args) throws Exception
    {
        ExecutorService pool = Executors.newFixedThreadPool(MAX_PARALLEL_TASKS);
        Future<CMTaskResult> future;
        // pool.shutdown();
        // pool.awaitTermination(5, TimeUnit.SECONDS);
        future = pool.submit(new Callable<CMTaskResult>()
        {

            @Override
            public CMTaskResult call() throws Exception
            {
                System.out.println("CMTaskExecutor.main(...).new Callable() {...}.call()");
                return null;
            }

        });
        future = pool.submit(new CMTaskHandler(null, null, null));
        Thread.currentThread().join();
        System.out.println(future.isDone());
    }
}
