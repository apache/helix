package org.apache.helix.tools;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;

public class ZKLogFormatter {
  private static final Logger LOG = Logger.getLogger(ZKLogFormatter.class);
  private static DateFormat dateTimeInstance = DateFormat.getDateTimeInstance(DateFormat.SHORT,
      DateFormat.LONG);
  private static HexBinaryAdapter adapter = new HexBinaryAdapter();
  private static String fieldDelim = ":";
  private static String fieldSep = " ";

  static BufferedWriter bw = null;

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2 && args.length != 3) {
      System.err.println("USAGE: LogFormatter <log|snapshot> log_file");
      System.exit(2);
    }

    if (args.length == 3) {
      bw = new BufferedWriter(new FileWriter(new File(args[2])));
    }

    if (args[0].equals("log")) {
      readTransactionLog(args[1]);
    } else if (args[0].equals("snapshot")) {
      readSnapshotLog(args[1]);
    }

    if (bw != null) {
      bw.close();
    }
  }

  private static void readSnapshotLog(String snapshotPath) throws Exception {
    FileInputStream fis = new FileInputStream(snapshotPath);
    BinaryInputArchive ia = BinaryInputArchive.getArchive(fis);
    Map<Long, Integer> sessions = new HashMap<Long, Integer>();
    DataTree dt = new DataTree();
    FileHeader header = new FileHeader();
    header.deserialize(ia, "fileheader");
    if (header.getMagic() != FileSnap.SNAP_MAGIC) {
      throw new IOException("mismatching magic headers " + header.getMagic() + " !=  "
          + FileSnap.SNAP_MAGIC);
    }
    SerializeUtils.deserializeSnapshot(dt, ia, sessions);

    if (bw != null) {
      bw.write(sessions.toString());
      bw.newLine();
    } else {
      System.out.println(sessions);
    }
    traverse(dt, 1, "/");

  }

  /*
   * Level order traversal
   */
  private static void traverse(DataTree dt, int startId, String startPath) throws Exception {
    LinkedList<Pair> queue = new LinkedList<Pair>();
    queue.add(new Pair(startPath, startId));
    while (!queue.isEmpty()) {
      Pair pair = queue.removeFirst();
      String path = pair._path;
      DataNode head = dt.getNode(path);
      Stat stat = new Stat();
      byte[] data = null;
      try {
        data = dt.getData(path, stat, null);
      } catch (NoNodeException e) {
        e.printStackTrace();
      }
      // print the node
      format(startId, pair, head, data);
      Set<String> children = head.getChildren();
      if (children != null) {
        for (String child : children) {
          String childPath;
          if (path.endsWith("/")) {
            childPath = path + child;
          } else {
            childPath = path + "/" + child;
          }
          queue.add(new Pair(childPath, startId));
        }
      }
      startId = startId + 1;
    }

  }

  static class Pair {

    private final String _path;
    private final int _parentId;

    public Pair(String path, int parentId) {
      _path = path;
      _parentId = parentId;
    }

  }

  private static void format(int id, Pair pair, DataNode head, byte[] data) throws Exception {
    String dataStr = "";
    if (data != null) {
      dataStr = new String(data).replaceAll("[\\s]+", "");
    }
    StringBuffer sb = new StringBuffer();
    // @formatter:off
    sb.append("id").append(fieldDelim).append(id).append(fieldSep);
    sb.append("parent").append(fieldDelim).append(pair._parentId).append(fieldSep);
    sb.append("path").append(fieldDelim).append(pair._path).append(fieldSep);
    sb.append("session").append(fieldDelim)
        .append("0x" + Long.toHexString(head.stat.getEphemeralOwner())).append(fieldSep);
    sb.append("czxid").append(fieldDelim).append("0x" + Long.toHexString(head.stat.getCzxid()))
        .append(fieldSep);
    sb.append("ctime").append(fieldDelim).append(head.stat.getCtime()).append(fieldSep);
    sb.append("mtime").append(fieldDelim).append(head.stat.getMtime()).append(fieldSep);
    sb.append("cmzxid").append(fieldDelim).append("0x" + Long.toHexString(head.stat.getMzxid()))
        .append(fieldSep);
    sb.append("pzxid").append(fieldDelim).append("0x" + Long.toHexString(head.stat.getPzxid()))
        .append(fieldSep);
    sb.append("aversion").append(fieldDelim).append(head.stat.getAversion()).append(fieldSep);
    sb.append("cversion").append(fieldDelim).append(head.stat.getCversion()).append(fieldSep);
    sb.append("version").append(fieldDelim).append(head.stat.getVersion()).append(fieldSep);
    sb.append("data").append(fieldDelim).append(dataStr).append(fieldSep);
    // @formatter:on

    if (bw != null) {
      bw.write(sb.toString());
      bw.newLine();
    } else {
      System.out.println(sb);
    }

  }

  private static void readTransactionLog(String logfilepath) throws FileNotFoundException,
      IOException, EOFException {
    FileInputStream fis = new FileInputStream(logfilepath);
    BinaryInputArchive logStream = BinaryInputArchive.getArchive(fis);
    FileHeader fhdr = new FileHeader();
    fhdr.deserialize(logStream, "fileheader");

    if (fhdr.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
      System.err.println("Invalid magic number for " + logfilepath);
      System.exit(2);
    }

    if (bw != null) {
      bw.write("ZooKeeper Transactional Log File with dbid " + fhdr.getDbid()
          + " txnlog format version " + fhdr.getVersion());
      bw.newLine();
    } else {
      System.out.println("ZooKeeper Transactional Log File with dbid " + fhdr.getDbid()
          + " txnlog format version " + fhdr.getVersion());
    }

    int count = 0;
    while (true) {
      long crcValue;
      byte[] bytes;
      try {
        crcValue = logStream.readLong("crcvalue");

        bytes = logStream.readBuffer("txnEntry");
      } catch (EOFException e) {
        if (bw != null) {
          bw.write("EOF reached after " + count + " txns.");
          bw.newLine();
        } else {
          System.out.println("EOF reached after " + count + " txns.");
        }

        break;
      }
      if (bytes.length == 0) {
        // Since we preallocate, we define EOF to be an
        // empty transaction
        if (bw != null) {
          bw.write("EOF reached after " + count + " txns.");
          bw.newLine();
        } else {
          System.out.println("EOF reached after " + count + " txns.");
        }

        return;
      }
      Checksum crc = new Adler32();
      crc.update(bytes, 0, bytes.length);
      if (crcValue != crc.getValue()) {
        throw new IOException("CRC doesn't match " + crcValue + " vs " + crc.getValue());
      }
      InputArchive iab = BinaryInputArchive.getArchive(new ByteArrayInputStream(bytes));
      TxnHeader hdr = new TxnHeader();
      Record txn = SerializeUtils.deserializeTxn(iab, hdr);
      if (bw != null) {
        bw.write(formatTransaction(hdr, txn));
        bw.newLine();
      } else {
        System.out.println(formatTransaction(hdr, txn));
      }

      if (logStream.readByte("EOR") != 'B') {
        LOG.error("Last transaction was partial.");
        throw new EOFException("Last transaction was partial.");
      }
      count++;
    }
  }

  static String op2String(int op) {
    switch (op) {
    case OpCode.notification:
      return "notification";
    case OpCode.create:
      return "create";
    case OpCode.delete:
      return "delete";
    case OpCode.exists:
      return "exists";
    case OpCode.getData:
      return "getDate";
    case OpCode.setData:
      return "setData";
    case OpCode.getACL:
      return "getACL";
    case OpCode.setACL:
      return "setACL";
    case OpCode.getChildren:
      return "getChildren";
    case OpCode.getChildren2:
      return "getChildren2";
    case OpCode.ping:
      return "ping";
    case OpCode.createSession:
      return "createSession";
    case OpCode.closeSession:
      return "closeSession";
    case OpCode.error:
      return "error";
    default:
      return "unknown " + op;
    }
  }

  private static String formatTransaction(TxnHeader header, Record txn) {
    StringBuilder sb = new StringBuilder();

    sb.append("time").append(fieldDelim).append(header.getTime());
    sb.append(fieldSep).append("session").append(fieldDelim).append("0x")
        .append(Long.toHexString(header.getClientId()));
    sb.append(fieldSep).append("cxid").append(fieldDelim).append("0x")
        .append(Long.toHexString(header.getCxid()));
    sb.append(fieldSep).append("zxid").append(fieldDelim).append("0x")
        .append(Long.toHexString(header.getZxid()));
    sb.append(fieldSep).append("type").append(fieldDelim).append(op2String(header.getType()));
    if (txn != null) {
      try {
        byte[] data = null;
        for (PropertyDescriptor pd : Introspector.getBeanInfo(txn.getClass())
            .getPropertyDescriptors()) {
          if (pd.getName().equalsIgnoreCase("data")) {
            data = (byte[]) pd.getReadMethod().invoke(txn);
            continue;
          }
          if (pd.getReadMethod() != null && !"class".equals(pd.getName())) {
            sb.append(fieldSep).append(pd.getDisplayName()).append(fieldDelim)
                .append(pd.getReadMethod().invoke(txn).toString().replaceAll("[\\s]+", ""));
          }
        }
        if (data != null) {
          sb.append(fieldSep).append("data").append(fieldDelim)
              .append(new String(data).replaceAll("[\\s]+", ""));
        }
      } catch (Exception e) {
        LOG.error("Error while retrieving bean property values for " + txn.getClass(), e);
      }
    }

    return sb.toString();
  }

}
