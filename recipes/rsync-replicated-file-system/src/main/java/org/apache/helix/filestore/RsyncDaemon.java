package org.apache.helix.filestore;


public class RsyncDaemon
{
  public boolean start()
  {
    ProcessBuilder pb = new ProcessBuilder("sudo", "rync", "--daemon");
    ExternalCommand externalCommand = new ExternalCommand(pb);
    try
    {
      int exitVal = externalCommand.waitFor();
      if(exitVal!=0){
        return true;
      }
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    return false;
  }

 

  public boolean stop()
  {
    return true;
  }
}
