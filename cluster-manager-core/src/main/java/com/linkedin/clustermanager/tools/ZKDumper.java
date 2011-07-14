package com.linkedin.clustermanager.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 * Dumps the Zookeeper file structure on to Disk
 * 
 * @author kgopalak
 * 
 */
@SuppressWarnings("static-access")
public class ZKDumper
{
  private ZkClient client;
  private FilenameFilter filter;
  static Options options;
  static
  {
    options = new Options();
    OptionGroup optionGroup = new OptionGroup();
    Option d = OptionBuilder.withLongOpt("download")
        .withDescription("Download from ZK to File System").create();
    d.setArgs(0);
    d.setRequired(true);
    Option u = OptionBuilder.withLongOpt("upload")
        .withDescription("Upload from File System to ZK").create();
    u.setArgs(0);
    u.setRequired(true);
    options.addOption(d);
    options.addOption(u);
    options.addOptionGroup(optionGroup);
    options.addOption("zkSvr", true, "Zookeeper address");
    options.addOption("zkpath", true, "Zookeeper path");
    options.addOption("fspath", true, "Path on local Filesystem to dump");
    options.addOption("h", "help", false, "Print this usage information");
    options.addOption("v", "verbose", false, "Print out VERBOSE information");
  }

  public ZKDumper(String zkAddress)
  {
    client = new ZkClient(zkAddress);
    ZkSerializer zkSerializer = new ZkSerializer()
    {

      @Override
      public byte[] serialize(Object arg0) throws ZkMarshallingError
      {
        return arg0.toString().getBytes();
      }

      @Override
      public Object deserialize(byte[] arg0) throws ZkMarshallingError
      {
        return new String(arg0);
      }
    };
    client.setZkSerializer(zkSerializer);
    filter = new FilenameFilter()
    {

      @Override
      public boolean accept(File dir, String name)
      {
        return !name.startsWith(".");
      }
    };
  }

  public static void main(String[] args) throws Exception
  {
    if (args == null || args.length == 0)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("java " + ZKDumper.class.getName(), options);
      System.exit(1);
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    cmd.hasOption("zkSvr");
    boolean download = cmd.hasOption("download");
    boolean upload = cmd.hasOption("upload");
    String zkAddress = cmd.getOptionValue("zkSvr");
    String zkPath = cmd.getOptionValue("zkPath");
    String fsPath = cmd.getOptionValue("fsPath");
    ZKDumper zkDump = new ZKDumper(zkAddress);
    if (download)
    {
      zkDump.download(zkPath, fsPath);
    }
    if (upload)
    {
      zkDump.download(zkPath, fsPath);
    }
  }

  public void upload(String zkPath, String fsPath) throws Exception
  {
    File file = new File(fsPath);
    System.out
        .println("Uploading " + file.getCanonicalPath() + " to " + zkPath);
    File[] children = file.listFiles(filter);
    if (children != null && children.length > 0)
    {
      client.createPersistent(zkPath, true);
      for (File child : children)
      {
        upload(zkPath + "/" + child, fsPath + "/" + child);
      }
    } else
    {
      BufferedReader bfr = null;
      try
      {
        bfr = new BufferedReader(new FileReader(file));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = bfr.readLine()) != null)
        {
          sb.append(line);
        }
        client.createPersistent(zkPath, sb.toString());
      } catch (Exception e)
      {
        throw e;
      } finally
      {
        if (bfr != null)
        {
          try
          {
            bfr.close();
          } catch (IOException e)
          {
          }
        }
      }
    }
  }

  public void download(String zkPath, String fsPath) throws Exception
  {
    System.out.println("Saving " + zkPath + " to "
        + new File(fsPath).getCanonicalPath());
    List<String> children = client.getChildren(zkPath);
    if (children != null && children.size() > 0)
    {
      new File(fsPath).mkdirs();
      for (String child : children)
      {
        download(zkPath + "/" + child, fsPath + "/" + child);
      }
    } else
    {
      FileWriter fileWriter = new FileWriter(fsPath);
      Object readData = client.readData(zkPath);
      fileWriter.write((String) readData);
      fileWriter.close();
    }
  }
}
