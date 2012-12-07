package org.apache.helix.filestore;

import java.io.IOException;

public class RsyncInvoker
{
  private Thread backgroundThread;
  private final String remoteHost;
  private final String remoteLogDir;
  private final String localLogDir;

  public RsyncInvoker(String remoteHost, String remoteBaseDir,
      String localBaseDir)
  {
    this.remoteHost = remoteHost;
    this.remoteLogDir = remoteBaseDir;
    this.localLogDir = localBaseDir;
  }

  public boolean rsync(String relativePath)
  {
    int exitVal = -1;
    try
    {
      ProcessBuilder pb = new ProcessBuilder( "rsync", 
           remoteLogDir+"/"+ relativePath , localLogDir);
      System.out.println("Rsyncing source:"+ remoteLogDir+"/"+ relativePath  + " dest:"+ localLogDir);
      ExternalCommand ec = new ExternalCommand(pb);
      ec.start();
      exitVal = ec.waitFor();
      if (exitVal != 0)
      {
        System.out.println("Failed to rsync "+ ec.getStringError());
      } else
      {
        return true;
      }
    } catch (IOException e)
    {
      e.printStackTrace();
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }

    return false;
  }
  public boolean stop(){
    if( backgroundThread!=null){
      backgroundThread.interrupt();
    }
    return true;
  }
  public boolean runInBackground()
  {
    backgroundThread = new Thread(new Runnable()
    {
      public void run()
      {
        try
        {
          int sleep = 1000;
          while (true)
          {
            int exitVal = -1;
            try
            {
              Thread.sleep(sleep);
              ProcessBuilder pb = new ProcessBuilder( "rsync", "-rvt",
                   remoteLogDir+"/", localLogDir);
              //System.out.println("Background rsync source:"+remoteLogDir+"/" +" dest:"+ localLogDir);
              ExternalCommand ec;
              ec = new ExternalCommand(pb);
              ec.start();
              exitVal = ec.waitFor();
              String stringError = ec.getStringError();
              if(stringError!=null && stringError.length()>0)
              System.err.println(stringError);
              //System.out.println(ec.getStringOutput());
              
            } catch (IOException e)
            {
              e.printStackTrace();
            }
            if (exitVal != 0)
            {
              
              sleep = Math.min(2 * sleep, 2 * 60 * 1000);
              System.out.println("Failed to rsync retrying in " + sleep / 1000
                  + " seconds");
            } else
            {
              sleep = 1000;
            }
          }
        } catch (InterruptedException e)
        {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
    });
    backgroundThread.start();
    return true;
  }
  public static void main(String[] args)
  {
    String remoteHost= "localhost";
    String remoteLogDir="data/localhost_12000/translog";
    String localLogDir="data/localhost_12001/translog";
    RsyncInvoker rsyncInvoker = new RsyncInvoker(remoteHost, remoteLogDir, localLogDir);
    rsyncInvoker.runInBackground();
  }
}
