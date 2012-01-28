package com.linkedin.helix.tools;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorCode;

public class CLMLogFileAppender extends FileAppender
{
  public CLMLogFileAppender()
  {
  }

  public CLMLogFileAppender(Layout layout, String filename, boolean append,
      boolean bufferedIO, int bufferSize) throws IOException
  {
    super(layout, filename, append, bufferedIO, bufferSize);
  }

  public CLMLogFileAppender(Layout layout, String filename, boolean append)
      throws IOException
  {
    super(layout, filename, append);
  }

  public CLMLogFileAppender(Layout layout, String filename) throws IOException
  {
    super(layout, filename);
  }

  public void activateOptions()
  {
    if (fileName != null)
    {
      try
      {
        fileName = getNewLogFileName();
        setFile(fileName, fileAppend, bufferedIO, bufferSize);
      } catch (Exception e)
      {
        errorHandler.error("Error while activating log options", e,
            ErrorCode.FILE_OPEN_FAILURE);
      }
    }
  }

  private String getNewLogFileName()
  {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
    String time = sdf.format(cal.getTime());

    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StackTraceElement main = stack[stack.length - 1];
    String mainClass = main.getClassName();

    return System.getProperty("user.home") + "/EspressoLogs/" + mainClass + "_"
        + time + ".txt";
  }
}
