package org.apache.helix.filestore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class Test
{
  public static void main(String[] args) throws InterruptedException
  {
    while(true){
    ExecutorService service = Executors.newFixedThreadPool(400);
    List<Callable<Object>> list = new ArrayList<Callable<Object>>();
    for (int i = 0; i < 256; i++)
    {
      MyRunnable runnable = new MyRunnable();
      list.add(runnable);
      new Thread(runnable).start();
      // service.submit(runnable);
    }
    
//    List<Future<Object>> invokeAll = service.invokeAll(list);
//    service.shutdownNow();
    System.out.println("RUN -------------");
    Thread.sleep(5000);
    }
  }
}

class MyRunnable implements Callable<Object>,Runnable
{

  // AtomicReference<Object> ref = new AtomicReference<Object>(new Object());
  class Ref
  {
    Object obj;

    public Ref(Object obj)
    {
      this.obj = obj;
    }

    void set(Object obj)
    {
      this.obj = obj;
    }

    Object get()
    {
      return obj;
    }
  }

  Ref ref = new Ref(new Object());
  Object lock = new Object();

  //@Override
  public Object call1() throws Exception
  {
    long start = System.currentTimeMillis();
    System.out.println(start);
    synchronized (lock)
    {

      if (ref.get() != null)
      {
        try
        {
          int sum = 0;
          List<Float> array = new ArrayList<Float>();
          for (int i = 0; i < 5000; i++)
          {
            array.add((float) Math.random());
          }
          Collections.sort(array);
        } catch (Exception e)
        {
          e.printStackTrace();
        }
        ref.set(null);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
    return new Object();

  }

  @Override
  public Object call() throws Exception
  {
    long start = System.currentTimeMillis();
    System.out.println(start);
    try
    {
//      int sum = 0;
//      List<Float> array = new ArrayList<Float>();
//      for (int i = 0; i < 5000; i++)
//      {
//        array.add((float) Math.random());
//      }
      //Collections.sort(array);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    ref.set(null);
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));
    return new Object();

  }

  @Override
  public void run()
  {
    long start = System.currentTimeMillis();
    System.out.println(start);
    try
    {
      int sum = 0;
      List<Float> array = new ArrayList<Float>();
      for (int i = 0; i < 5000; i++)
      {
        array.add((float) Math.random());
      }
      Collections.sort(array);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    ref.set(null);
    long end = System.currentTimeMillis();
    System.out.println("time:" + (end - start));    
  }
}

