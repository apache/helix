package com.linkedin.clustermanager.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import com.linkedin.clustermanager.agent.zk.ZkClient;
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
  private ZkClient       client;
  private FilenameFilter filter;
  static Options         options;
  
  private static final String SCHEMA_FILE_SUFFIX = ".json";
  
  static
  {
    options = new Options();
    OptionGroup optionGroup = new OptionGroup();
    Option d =
        OptionBuilder.withLongOpt("download")
                     .withDescription("Download from ZK to File System")
                     .create();
    d.setArgs(0);

    Option u =
        OptionBuilder.withLongOpt("upload")
                     .withDescription("Upload from File System to ZK")
                     .create();
    u.setArgs(0);
    
    Option del = 
        OptionBuilder.withLongOpt("delete")
                     .withDescription("Delete given path from ZK")
                     .create();
    
    optionGroup.setRequired(true);
    optionGroup.addOption(d);
    optionGroup.addOption(u);
    optionGroup.addOption(del);
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
    boolean del = cmd.hasOption("delete");
    String zkAddress = cmd.getOptionValue("zkSvr");
    String zkPath = cmd.getOptionValue("zkpath");
    String fsPath = cmd.getOptionValue("fspath");
    
    ZKDumper zkDump = new ZKDumper(zkAddress);
    if (download)
    {
      zkDump.download(zkPath, fsPath + zkPath);
    }
    if (upload)
    {
      zkDump.upload(zkPath, fsPath);
    }
    if (del)
    {
      zkDump.delete(zkPath);
    }
  }

  private void delete(String zkPath)
  {
    client.deleteRecursive(zkPath);
    
  }

  public void upload(String zkPath, String fsPath) throws Exception
  {
    File file = new File(fsPath);
    System.out.println("Uploading " + file.getCanonicalPath() + " to " + zkPath);
    
    if (file.isDirectory())
    {
      File[] children = file.listFiles(filter);
      client.createPersistent(zkPath, true);
      if (children != null && children.length > 0)
      {
        
        for (File child : children)
        {
          upload(zkPath + "/" + child.getName(), fsPath + "/" + child.getName());
        }
      }else{
        
      }
    }
    else
    {
      BufferedReader bfr = null;
      try
      {
        bfr = new BufferedReader(new FileReader(file));
        StringBuilder sb = new StringBuilder();
        String line;
        String recordDelimiter= "";
        while ((line = bfr.readLine()) != null)
        {
          sb.append(recordDelimiter).append(line);
          recordDelimiter = "\n";
        }
        client.createPersistent(zkPath, sb.toString());
      }
      catch (Exception e)
      {
        throw e;
      }
      finally
      {
        if (bfr != null)
        {
          try
          {
            bfr.close();
          }
          catch (IOException e)
          {
          }
        }
      }
    }
  }

  public void download(String zkPath, String fsPath) throws Exception
  {
    System.out.println("Saving " + zkPath + " to " + new File(fsPath).getCanonicalPath());
    List<String> children = client.getChildren(zkPath);
    if (children != null && children.size() > 0)
    {
      new File(fsPath).mkdirs();
      for (String child : children)
      {
        download(zkPath + "/" + child, fsPath + "/" + child);
      }
    }
    else
    {
      FileWriter fileWriter = new FileWriter(fsPath + SCHEMA_FILE_SUFFIX);
      Object readData = client.readData(zkPath);
      if(readData != null)
      {
        fileWriter.write((String) readData);
      }
      fileWriter.close();
    }
  }
}
