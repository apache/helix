package com.linkedin.helix.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageState;

public class ZkLogAnalyzer
{
  private static Logger           LOG           = Logger.getLogger(ZkLogAnalyzer.class);
  private static boolean dump= false;;
  final static ZNRecordSerializer _deserializer = new ZNRecordSerializer();

  static class Stats
  {
    int msgSentCount        = 0;
    int msgSentCount_O2S    = 0; // Offline to Slave
    int msgSentCount_S2M    = 0; // Slave to Master
    int msgSentCount_M2S    = 0; // Master to Slave
    int msgDeleteCount      = 0;
    int msgModifyCount      = 0;
    int curStateCreateCount = 0;
    int curStateUpdateCount = 0;
    int extViewCreateCount  = 0;
    int extViewUpdateCount  = 0;
  }

  static String getAttributeValue(String line, String attribute)
  {
    if(line==null)return null;
    String[] parts = line.split("\\s");
    if (parts != null && parts.length > 0)
    {
      for (int i = 0; i < parts.length; i++)
      {
        if (parts[i].startsWith(attribute))
        {
          String val = parts[i].substring(attribute.length());
          return val;
        }
      }
    }
    return null;
  }

  static String findLastCSUpdateBetween(List<String> csUpdateLines, long start, long end)
  {
    long lastCSUpdateTimestamp = Long.MIN_VALUE;
    String lastCSUpdateLine = null;
    for (String line : csUpdateLines)
    {
      // ZNRecord record = getZNRecord(line);
      long timestamp = Long.parseLong(getAttributeValue(line, "time:"));
      if (timestamp >= start && timestamp <= end && timestamp > lastCSUpdateTimestamp)
      {
        lastCSUpdateTimestamp = timestamp;
        lastCSUpdateLine = line;
      }
    }
    assert (lastCSUpdateLine != null) : "No CS update between " + start + " - " + end;
    return lastCSUpdateLine;
  }

  static ZNRecord getZNRecord(String line)
  {
    ZNRecord record = null;
    String value = getAttributeValue(line, "data:");
    if (value != null)
    {
      record = (ZNRecord) _deserializer.deserialize(value.getBytes());
      // if (record == null)
      // {
      // System.out.println(line);
      // }
    }
    return record;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 3)
    {
      System.err.println("USAGE: ZkLogAnalyzer zkLogDir clusterName zkAddr");
      System.exit(1);
    }

    // get create-timestamp of "/" + clusterName
    // find all zk logs after that create-timestamp and parse them
    // save parsed log in /tmp/zkLogAnalyzor_zklog.parsed0,1,2...

    String zkLogDir = args[0];
    String clusterName = args[1];
    String zkAddr = args[2];
    ZkClient zkClient = new ZkClient(zkAddr);
    Stat clusterCreateStat = zkClient.getStat("/" + clusterName);
    System.out.println(clusterName + " created at " + clusterCreateStat.getCtime());
    while (zkLogDir.endsWith("/"))
    {
      zkLogDir = zkLogDir.substring(0, zkLogDir.length() - 1);
    }
    if (!zkLogDir.endsWith("/version-2"))
    {
      zkLogDir = zkLogDir + "/version-2";
    }
    File dir = new File(zkLogDir);
    File[] zkLogs = dir.listFiles(new FileFilter()
    {

      @Override
      public boolean accept(File file)
      {
        return file.isFile() && (file.getName().indexOf("log") != -1);
      }
    });

    // lastModified time -> zkLog
    TreeMap<Long, String> lastZkLogs = new TreeMap<Long, String>();
    for (File file : zkLogs)
    {
      if (file.lastModified() > clusterCreateStat.getCtime())
      {
        lastZkLogs.put(file.lastModified(), file.getAbsolutePath());
      }
    }

    List<String> parsedZkLogs = new ArrayList<String>();
    int i = 0;
    System.out.println("zk logs last modified later than " + clusterCreateStat.getCtime());
    for (Long lastModified : lastZkLogs.keySet())
    {
      String fileName = lastZkLogs.get(lastModified);
      System.out.println(lastModified + ": "
          + (fileName.substring(fileName.lastIndexOf('/') + 1)));

      String parsedFileName = "zkLogAnalyzor_zklog.parsed" + i;
      i++;
      ZKLogFormatter.main(new String[] { "log", fileName, parsedFileName });
      parsedZkLogs.add(parsedFileName);
    }

    // sessionId -> create liveInstance line
    Map<String, String> sessionMap = new HashMap<String, String>();

    // message send lines in time order
    // List<String> sendMessageLines = new ArrayList<String>();

    // CS update lines in time order
    List<String> csUpdateLines = new ArrayList<String>();

    String leaderSession = null;

    System.out.println();
    Stats stats = new Stats();
    long lastTestStartTimestamp = Long.MAX_VALUE;
    long controllerStartTime =0;
    for (String parsedZkLog : parsedZkLogs)
    {

      FileInputStream fis = new FileInputStream(parsedZkLog);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));

      String inputLine;
      while ((inputLine = br.readLine()) != null)
      {
        if(dump ==true ){
          System.err.println(inputLine.replaceAll("data:.*", ""));
        }
        String timestamp = getAttributeValue(inputLine, "time:");
        if (timestamp == null)
        {
          continue;
        }
        long timestampVal = Long.parseLong(timestamp);
        if (timestampVal < clusterCreateStat.getCtime())
        {
          continue;
        }
        if (inputLine.indexOf("/start_disable") != -1){
          dump =true;
        }
        if (inputLine.indexOf("/" + clusterName + "/CONFIGS/CLUSTER/verify") != -1)
        {
          String type = getAttributeValue(inputLine, "type:");
          if (type.equals("delete"))
          {
            System.out.println(timestamp + ": verify done");
            System.out.println("lastTestStartTimestamp:"+ lastTestStartTimestamp);
            String lastCSUpdateLine =
                findLastCSUpdateBetween(csUpdateLines,
                                        lastTestStartTimestamp,
                                        timestampVal);
            long lastCSUpdateTimestamp =
                Long.parseLong(getAttributeValue(lastCSUpdateLine, "time:"));
            System.out.println("Last CS Update:"+ lastCSUpdateTimestamp);
            
            System.out.println("state transition latency: "+ 
                + (lastCSUpdateTimestamp - lastTestStartTimestamp) + "ms");

            System.out.println("state transition latency since controller start: "+ 
                + (lastCSUpdateTimestamp - controllerStartTime) + "ms");
            
            System.out.println("Create MSG\t" + stats.msgSentCount + "\t"
                + stats.msgSentCount_O2S + "\t" + stats.msgSentCount_S2M + "\t"
                + stats.msgSentCount_M2S);
            System.out.println("Modify MSG\t" + stats.msgModifyCount);
            System.out.println("Delete MSG\t" + stats.msgDeleteCount);
            System.out.println("Create CS\t" + stats.curStateCreateCount);
            System.out.println("Update CS\t" + stats.curStateUpdateCount);
            System.out.println("Create EV\t" + stats.extViewCreateCount);
            System.out.println("Update EV\t" + stats.extViewUpdateCount);

            System.out.println();
            stats = new Stats();
            lastTestStartTimestamp = Long.MAX_VALUE;
          }
        }
        else if (inputLine.indexOf("/" + clusterName + "/LIVEINSTANCES/") != -1)
        {
          if (timestampVal < lastTestStartTimestamp)
          {
            System.out.println("SETTING lastTestStartTimestamp to "+timestampVal + " line:"+ inputLine);
            lastTestStartTimestamp = timestampVal;
          }

          ZNRecord record = getZNRecord(inputLine);
          LiveInstance liveInstance = new LiveInstance(record);
          String session = getAttributeValue(inputLine, "session:");
          sessionMap.put(session, inputLine);
          System.out.println(timestamp + ": create LIVEINSTANCE "
              + liveInstance.getInstanceName());
        }
        else if (inputLine.indexOf("closeSession") != -1)
        {
          // String timestamp = getAttributeValue(inputLine, "time:");
          String session = getAttributeValue(inputLine, "session:");
          if (sessionMap.containsKey(session))
          {
            if (timestampVal < lastTestStartTimestamp)
            {
              System.out.println("SETTING lastTestStartTimestamp to "+timestampVal + " line:"+ inputLine);
               lastTestStartTimestamp = timestampVal;
            }
            String line = sessionMap.get(session);
            ZNRecord record = getZNRecord(line);
            LiveInstance liveInstance = new LiveInstance(record);

            System.out.println(timestamp + ": close session "
                + liveInstance.getInstanceName());
            dump =true;
          }
        }
        else if (inputLine.indexOf("/" + clusterName + "/CONTROLLER/LEADER") != -1)
        {
          // leaderLine = inputLine;
          ZNRecord record = getZNRecord(inputLine);
          LiveInstance liveInstance = new LiveInstance(record);
          String session = getAttributeValue(inputLine, "session:");
          leaderSession = session;
          controllerStartTime = Long.parseLong(getAttributeValue(inputLine, "time:"));
          sessionMap.put(session, inputLine);
          System.out.println(timestamp + ": create LEADER "
              + liveInstance.getInstanceName());
        }
        else if (inputLine.indexOf("/" + clusterName + "/") != -1
            && inputLine.indexOf("/CURRENTSTATES/") != -1)
        {
          String type = getAttributeValue(inputLine, "type:");
          if (type.equals("create"))
          {
            stats.curStateCreateCount++;
          }
          else if (type.equals("setData"))
          {
            String path = getAttributeValue(inputLine, "path:");
            csUpdateLines.add(inputLine);
            stats.curStateUpdateCount++;
           // getAttributeValue(line, "data");
            System.out.println("Update currentstate:"+ new Timestamp(Long.parseLong(timestamp)) + ":" + timestamp + " path:"+ path );
          }
        }
        else if (inputLine.indexOf("/" + clusterName + "/EXTERNALVIEW/") != -1)
        {
          String session = getAttributeValue(inputLine, "session:");
          if (session.equals(leaderSession))
          {
            String type = getAttributeValue(inputLine, "type:");
            if (type.equals("create"))
            {
              stats.extViewCreateCount++;
            }
            else if (type.equals("setData"))
            {
              stats.extViewUpdateCount++;
            }
          }

          // pos = inputLine.indexOf("EXTERNALVIEW");
          // pos = inputLine.indexOf("data:{", pos);
          // if (pos != -1)
          // {
          // String timestamp = getAttributeValue(inputLine, "time:");
          // ZNRecord record =
          // (ZNRecord) _deserializer.deserialize(inputLine.substring(pos + 5)
          // .getBytes());
          // ExternalView extView = new ExternalView(record);
          // int masterCnt = ClusterStateVerifier.countStateNbInExtView(extView,
          // "MASTER");
          // int slaveCnt = ClusterStateVerifier.countStateNbInExtView(extView, "SLAVE");
          // if (masterCnt == 1200)
          // {
          // System.out.println(timestamp + ": externalView " + extView.getResourceName()
          // + " has " + masterCnt + " MASTER, " + slaveCnt + " SLAVE");
          // }
          // }
        }
        else if (inputLine.indexOf("/" + clusterName + "/") != -1
            && inputLine.indexOf("/MESSAGES/") != -1)
        {
          String type = getAttributeValue(inputLine, "type:");

          if (type.equals("create"))
          {
            ZNRecord record = getZNRecord(inputLine);
            Message msg = new Message(record);
            String sendSession = getAttributeValue(inputLine, "session:");
            if (sendSession.equals(leaderSession)
                && msg.getMsgType().equals("STATE_TRANSITION")
                && msg.getMsgState() == MessageState.NEW)
            {
              // sendMessageLines.add(inputLine);
              stats.msgSentCount++;

              if (msg.getFromState().equals("OFFLINE")
                  && msg.getToState().equals("SLAVE"))
              {
                stats.msgSentCount_O2S++;
              }
              else if (msg.getFromState().equals("SLAVE")
                  && msg.getToState().equals("MASTER"))
              {
                stats.msgSentCount_S2M++;
              }
              else if (msg.getFromState().equals("MASTER")
                  && msg.getToState().equals("SLAVE"))
              {
                stats.msgSentCount_M2S++;
              }
              System.out.println("Message create:"+new Timestamp(Long.parseLong(timestamp)));
            }

            // pos = inputLine.indexOf("MESSAGES");
            // pos = inputLine.indexOf("data:{", pos);
            // if (pos != -1)
            // {
            //
            // byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
            // ZNRecord record = (ZNRecord) _deserializer.deserialize(msgBytes);
            // Message msg = new Message(record);
            // MessageState msgState = msg.getMsgState();
            // String msgType = msg.getMsgType();
            // if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.NEW)
            // {
            // if (!msgs.containsKey(msg.getMsgId()))
            // {
            // msgs.put(msg.getMsgId(), new MsgItem(Long.parseLong(timestamp), msg));
            // }
            // else
            // {
            // LOG.error("msg: " + msg.getMsgId() + " already sent");
            // }
            //
            // System.out.println(timestamp + ": sendMsg " + msg.getPartitionName() + "("
            // + msg.getFromState() + "->" + msg.getToState() + ") to "
            // + msg.getTgtName() + ", size: " + msgBytes.length);
            // }
            // }
          }
          else if (type.equals("setData"))
          {
            stats.msgModifyCount++;
            // pos = inputLine.indexOf("MESSAGES");
            // pos = inputLine.indexOf("data:{", pos);
            // if (pos != -1)
            // {
            //
            // byte[] msgBytes = inputLine.substring(pos + 5).getBytes();
            // ZNRecord record = (ZNRecord) _deserializer.deserialize(msgBytes);
            // Message msg = new Message(record);
            // MessageState msgState = msg.getMsgState();
            // String msgType = msg.getMsgType();
            // if (msgType.equals("STATE_TRANSITION") && msgState == MessageState.READ)
            // {
            // if (!msgs.containsKey(msg.getMsgId()))
            // {
            // LOG.error("msg: " + msg.getMsgId() + " never sent");
            // }
            // else
            // {
            // MsgItem msgItem = msgs.get(msg.getMsgId());
            // if (msgItem.readTime == 0)
            // {
            // msgItem.readTime = Long.parseLong(timestamp);
            // msgs.put(msg.getMsgId(), msgItem);
            // // System.out.println(timestamp + ": readMsg " + msg.getPartitionName()
            // // + "("
            // // + msg.getFromState() + "->" + msg.getToState() + ") to "
            // // + msg.getTgtName() + ", latency: " + (msgItem.readTime -
            // // msgItem.sendTime));
            // }
            // }
            //
            // }
            // }
          }
          else if (type.equals("delete"))
          {
            stats.msgDeleteCount++;
            // String msgId = path.substring(path.lastIndexOf('/') + 1);
            // if (msgs.containsKey(msgId))
            // {
            // MsgItem msgItem = msgs.get(msgId);
            // Message msg = msgItem.msg;
            // msgItem.deleteTime = Long.parseLong(timestamp);
            // msgs.put(msgId, msgItem);
            // msgItem.latency = msgItem.deleteTime - msgItem.sendTime;
            // System.out.println(timestamp + ": delMsg " + msg.getPartitionName() + "("
            // + msg.getFromState() + "->" + msg.getToState() + ") to "
            // + msg.getTgtName() + ", latency: " + msgItem.latency);
            // }
            // else
            // {
            // // messages other than STATE_TRANSITION message
            // // LOG.error("msg: " + msgId + " never sent");
            // }
          }
        }
      } // end of [br.readLine()) != null]
    }
  }
}