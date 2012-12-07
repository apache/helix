package org.apache.helix.filestore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;

/**
 * Processes the change log and invokes rsync for every change on the remote
 * machine
 * 
 * @author kgopalak
 * 
 */
public class ChangeLogProcessor implements Runnable
{
  private final ChangeLogReader reader;
  RsyncInvoker rsyncInvoker;
  private AtomicBoolean shutdownRequested;
  private CheckpointFile checkpointFile;
  private Thread thread;

  public ChangeLogProcessor(ChangeLogReader reader, String remoteHost,
      String remoteBaseDir, String localBaseDir, String checkpointDirPath)
      throws Exception
  {
    this.reader = reader;
    checkpointFile = new CheckpointFile(checkpointDirPath);

    shutdownRequested = new AtomicBoolean(false);
    rsyncInvoker = new RsyncInvoker(remoteHost, remoteBaseDir, localBaseDir);
  }

  public void start()
  {
    thread = new Thread(this);
    thread.start();
  }

  public void run()
  {
    try
    {
      ChangeRecord lastRecordProcessed = checkpointFile.findLastRecordProcessed();
      do
      {
        try
        {
          List<ChangeRecord> changes = reader
              .getChangeSince(lastRecordProcessed);
          Set<String> paths = getRemotePathsToSync(changes);
          for (String path : paths)
          {
            rsyncInvoker.rsync(path);
          }
          lastRecordProcessed = changes.get(changes.size() - 1);
          checkpointFile.checkpoint(lastRecordProcessed);
        } catch (Exception e)
        {
          e.printStackTrace();
        }
      } while (!shutdownRequested.get());
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }



  private Set<String> getRemotePathsToSync(List<ChangeRecord> changes)
  {
    Set<String> paths = new TreeSet<String>();
    for (ChangeRecord change : changes)
    {
      paths.add(change.file);
    }
    return paths;
  }

  public void stop()
  {
    shutdownRequested.set(true);
    thread.interrupt();
  }

}
