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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * utility for grep zk transaction/snapshot logs
 * - to grep a pattern by t1 use:
 * zkgrep --zkCfg zkCfg --by t1 --pattern patterns...
 * - to grep a pattern between t1 and t2 use:
 * zkgrep --zkCfg zkCfg --between t1 t2 --pattern patterns...
 * for example, to find fail-over latency between t1 and t2, use:
 * 1) zkgrep --zkCfg zkCfg --by t1 --pattern "/{cluster}/LIVEINSTNCES/" | grep {fail-node}
 * 2) zkgrep --zkCfg zkCfg --between t1 t2 --pattern "closeSession" | grep {fail-node session-id}
 * 3) zkgrep --zkCfg zkCfg --between t1 t2 --pattern "/{cluster}" | grep "CURRENTSTATES" |
 * grep "setData" | tail -1
 * fail-over latency = timestamp difference between 2) and 3)
 */
public class ZkGrep {
  private static Logger LOG = Logger.getLogger(ZkGrep.class);

  private static final String zkCfg = "zkCfg";
  private static final String pattern = "pattern";
  private static final String by = "by";
  private static final String between = "between";

  public static final String log = "log";
  public static final String snapshot = "snapshot";

  private static final String gzSuffix = ".gz";

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions() {
    Option zkCfgOption =
        OptionBuilder.hasArgs(1).isRequired(false).withLongOpt(zkCfg).withArgName("zoo.cfg")
            .withDescription("provide zoo.cfg").create();

    Option patternOption =
        OptionBuilder.hasArgs().isRequired(true).withLongOpt(pattern)
            .withArgName("grep-patterns...").withDescription("provide patterns (required)")
            .create();

    Option betweenOption =
        OptionBuilder.hasArgs(2).isRequired(false).withLongOpt(between)
            .withArgName("t1 t2 (timestamp in ms or yyMMdd_hhmmss_SSS)")
            .withDescription("grep between t1 and t2").create();

    Option byOption =
        OptionBuilder.hasArgs(1).isRequired(false).withLongOpt(by)
            .withArgName("t (timestamp in ms or yyMMdd_hhmmss_SSS)").withDescription("grep by t")
            .create();

    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(betweenOption);
    group.addOption(byOption);

    Options options = new Options();
    options.addOption(zkCfgOption);
    options.addOption(patternOption);
    options.addOptionGroup(group);
    return options;
  }

  /**
   * get zk transaction log dir and zk snapshot log dir
   * @param zkCfgFile
   * @return String[0]: zk-transaction-log-dir, String[1]: zk-snapshot-dir
   */
  static String[] getZkDataDirs(String zkCfgFile) {
    String[] zkDirs = new String[2];

    FileInputStream fis = null;
    BufferedReader br = null;
    try {
      fis = new FileInputStream(zkCfgFile);
      br = new BufferedReader(new InputStreamReader(fis));

      String line;
      while ((line = br.readLine()) != null) {
        String key = "dataDir=";
        if (line.startsWith(key)) {
          zkDirs[1] = zkDirs[0] = line.substring(key.length()) + "/version-2";
        }

        key = "dataLogDir=";
        if (line.startsWith(key)) {
          zkDirs[0] = line.substring(key.length()) + "/version-2";
        }
      }
    } catch (Exception e) {
      LOG.error("exception in read file: " + zkCfgFile, e);
    } finally {
      try {
        if (br != null) {
          br.close();
        }

        if (fis != null) {
          fis.close();
        }

      } catch (Exception e) {
        LOG.error("exception in closing file: " + zkCfgFile, e);
      }
    }

    return zkDirs;
  }

  // debug
  static void printFiles(File[] files) {
    System.out.println("START print");
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      System.out.println(file.getName() + ", " + file.lastModified());
    }
    System.out.println("END print");
  }

  /**
   * get files under dir in order of last modified time
   * @param dir
   * @param pattern
   * @return
   */
  static File[] getSortedFiles(String dirPath, final String pattern) {
    File dir = new File(dirPath);
    File[] files = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File file) {
        return file.isFile() && (file.getName().indexOf(pattern) != -1);
      }
    });

    Arrays.sort(files, new Comparator<File>() {

      @Override
      public int compare(File o1, File o2) {
        int sign = (int) Math.signum(o1.lastModified() - o2.lastModified());
        return sign;
      }

    });
    return files;
  }

  /**
   * get value for an attribute in a parsed zk log; e.g.
   * "time:1384984016778 session:0x14257d1d17e0004 cxid:0x5 zxid:0x46899 type:error err:-101"
   * given "time" return "1384984016778"
   * @param line
   * @param attribute
   * @return value
   */
  static String getAttributeValue(String line, String attribute) {
    if (line == null) {
      return null;
    }

    if (!attribute.endsWith(":")) {
      attribute = attribute + ":";
    }

    String[] parts = line.split("\\s");
    if (parts != null && parts.length > 0) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].startsWith(attribute)) {
          String val = parts[i].substring(attribute.length());
          return val;
        }
      }
    }
    return null;
  }

  static long getTimestamp(String line) {
    String timestamp = getAttributeValue(line, "time");
    return Long.parseLong(timestamp);
  }

  /**
   * parse a time string either in timestamp form or "yyMMdd_hhmmss_SSS" form
   * @param time
   * @return timestamp or -1 on error
   */
  static long parseTimeString(String time) {
    try {
      return Long.parseLong(time);
    } catch (NumberFormatException e) {
      try {
        SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd_hhmmss_SSS");
        Date date = formatter.parse(time);
        return date.getTime();
      } catch (java.text.ParseException ex) {
        LOG.error("fail to parse time string: " + time, e);
      }
    }
    return -1;
  }

  public static void grepZkLog(File zkLog, long start, long end, String... patterns) {
    FileInputStream fis = null;
    BufferedReader br = null;
    try {
      fis = new FileInputStream(zkLog);
      br = new BufferedReader(new InputStreamReader(fis));

      String line;
      while ((line = br.readLine()) != null) {
        try {
          long timestamp = getTimestamp(line);
          if (timestamp > end) {
            break;
          }

          if (timestamp < start) {
            continue;
          }

          boolean match = true;
          for (String pattern : patterns) {
            if (line.indexOf(pattern) == -1) {
              match = false;
              break;
            }
          }

          if (match) {
            System.out.println(line);
          }

        } catch (NumberFormatException e) {
          // ignore
        }
      }
    } catch (Exception e) {
      LOG.error("exception in grep zk-log: " + zkLog, e);
    } finally {
      try {
        if (br != null) {
          br.close();
        }

        if (fis != null) {
          fis.close();
        }

      } catch (Exception e) {
        LOG.error("exception in closing zk-log: " + zkLog, e);
      }
    }
  }

  public static void grepZkLogDir(List<File> parsedZkLogs, long start, long end, String... patterns) {
    for (File file : parsedZkLogs) {
      grepZkLog(file, start, end, patterns);

    }

  }

  public static void grepZkSnapshot(File zkSnapshot, String... patterns) {
    FileInputStream fis = null;
    BufferedReader br = null;
    try {
      fis = new FileInputStream(zkSnapshot);
      br = new BufferedReader(new InputStreamReader(fis));

      String line;
      while ((line = br.readLine()) != null) {
        try {
          boolean match = true;
          for (String pattern : patterns) {
            if (line.indexOf(pattern) == -1) {
              match = false;
              break;
            }
          }

          if (match) {
            System.out.println(line);
          }

        } catch (NumberFormatException e) {
          // ignore
        }
      }
    } catch (Exception e) {
      LOG.error("exception in grep zk-snapshot: " + zkSnapshot, e);
    } finally {
      try {
        if (br != null) {
          br.close();
        }

        if (fis != null) {
          fis.close();
        }

      } catch (Exception e) {
        LOG.error("exception in closing zk-snapshot: " + zkSnapshot, e);
      }
    }
  }

  /**
   * guess zoo.cfg dir
   * @return absolute path to zoo.cfg
   */
  static String guessZkCfgDir() {
    // TODO impl this
    return null;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ZkGrep.class.getName(), cliOptions);
  }

  /**
   * parse zk-transaction-logs between start and end, if not already parsed
   * @param zkLogDir
   * @param start
   * @param end
   * @return list of parsed zklogs between start and end, in order of last modified timestamp
   */
  static List<File> parseZkLogs(String zkLogDir, long start, long end) {
    File zkParsedDir = new File(String.format("%s/zklog-parsed", System.getProperty("user.home")));
    File[] zkLogs = getSortedFiles(zkLogDir, log);
    // printFiles(zkDataFiles);
    List<File> parsedZkLogs = new ArrayList<File>();

    boolean stop = false;
    for (File zkLog : zkLogs) {
      if (stop) {
        break;
      }

      if (zkLog.lastModified() < start) {
        continue;
      }

      if (zkLog.lastModified() > end) {
        stop = true;
      }

      try {
        File parsedZkLog = new File(zkParsedDir, stripGzSuffix(zkLog.getName()) + ".parsed");
        if (!parsedZkLog.exists() || parsedZkLog.lastModified() <= zkLog.lastModified()) {

          if (zkLog.getName().endsWith(gzSuffix)) {
            // copy and gunzip it
            FileUtils.copyFileToDirectory(zkLog, zkParsedDir);
            File zkLogGz = new File(zkParsedDir, zkLog.getName());
            File tmpZkLog = gunzip(zkLogGz);

            // parse gunzip file
            ZKLogFormatter.main(new String[] {
                log, tmpZkLog.getAbsolutePath(), parsedZkLog.getAbsolutePath()
            });

            // delete it
            zkLogGz.delete();
            tmpZkLog.delete();
          } else {
            // parse it directly
            ZKLogFormatter.main(new String[] {
                log, zkLog.getAbsolutePath(), parsedZkLog.getAbsolutePath()
            });
          }
        }
        parsedZkLogs.add(parsedZkLog);
      } catch (Exception e) {
        LOG.error("fail to parse zkLog: " + zkLog, e);
      }
    }

    return parsedZkLogs;
  }

  /**
   * Strip off a .gz suffix if any
   * @param filename
   * @return
   */
  static String stripGzSuffix(String filename) {
    if (filename.endsWith(gzSuffix)) {
      return filename.substring(0, filename.length() - gzSuffix.length());
    }
    return filename;
  }

  /**
   * Gunzip a file
   * @param zipFile
   * @return
   */
  static File gunzip(File zipFile) {
    File outputFile = new File(stripGzSuffix(zipFile.getAbsolutePath()));

    byte[] buffer = new byte[1024];

    try {

      GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(zipFile));
      FileOutputStream out = new FileOutputStream(outputFile);

      int len;
      while ((len = gzis.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }

      gzis.close();
      out.close();

      return outputFile;
    } catch (IOException e) {
      LOG.error("fail to gunzip file: " + zipFile, e);
    }

    return null;
  }

  /**
   * parse the last zk-snapshots by by-time, if not already parsed
   * @param zkSnapshotDir
   * @param byTime
   * @return File array which the first element is the last zk-snapshot by by-time and the second
   *         element is its parsed file
   */
  static File[] parseZkSnapshot(String zkSnapshotDir, long byTime) {
    File[] retFiles = new File[2];
    File zkParsedDir = new File(String.format("%s/zklog-parsed", System.getProperty("user.home")));
    File[] zkSnapshots = getSortedFiles(zkSnapshotDir, snapshot);
    // printFiles(zkDataFiles);
    File lastZkSnapshot = null;
    for (int i = 0; i < zkSnapshots.length; i++) {
      File zkSnapshot = zkSnapshots[i];
      if (zkSnapshot.lastModified() >= byTime) {
        break;
      }
      lastZkSnapshot = zkSnapshot;
      retFiles[0] = lastZkSnapshot;
    }

    try {
      File parsedZkSnapshot =
          new File(zkParsedDir, stripGzSuffix(lastZkSnapshot.getName()) + ".parsed");
      if (!parsedZkSnapshot.exists()
          || parsedZkSnapshot.lastModified() <= lastZkSnapshot.lastModified()) {

        if (lastZkSnapshot.getName().endsWith(gzSuffix)) {
          // copy and gunzip it
          FileUtils.copyFileToDirectory(lastZkSnapshot, zkParsedDir);
          File lastZkSnapshotGz = new File(zkParsedDir, lastZkSnapshot.getName());
          File tmpLastZkSnapshot = gunzip(lastZkSnapshotGz);

          // parse gunzip file
          ZKLogFormatter.main(new String[] {
              snapshot, tmpLastZkSnapshot.getAbsolutePath(), parsedZkSnapshot.getAbsolutePath()
          });

          // delete it
          lastZkSnapshotGz.delete();
          tmpLastZkSnapshot.delete();
        } else {
          // parse it directly
          ZKLogFormatter.main(new String[] {
              snapshot, lastZkSnapshot.getAbsolutePath(), parsedZkSnapshot.getAbsolutePath()
          });
        }

      }
      retFiles[1] = parsedZkSnapshot;
      return retFiles;
    } catch (Exception e) {
      LOG.error("fail to parse zkSnapshot: " + lastZkSnapshot, e);
    }

    return null;
  }

  public static void processCommandLineArgs(String[] cliArgs) {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: " + pe);
      printUsage(cliOptions);
      System.exit(1);
    }

    String zkCfgDirValue = null;
    String zkCfgFile = null;

    if (cmd.hasOption(zkCfg)) {
      zkCfgDirValue = cmd.getOptionValue(zkCfg);
    }

    if (zkCfgDirValue == null) {
      zkCfgDirValue = guessZkCfgDir();
    }

    if (zkCfgDirValue == null) {
      LOG.error("couldn't figure out path to zkCfg file");
      System.exit(1);
    }

    // get zoo.cfg path from cfg-dir
    zkCfgFile = zkCfgDirValue;
    if (!zkCfgFile.endsWith(".cfg")) {
      // append with default zoo.cfg
      zkCfgFile = zkCfgFile + "/zoo.cfg";
    }

    if (!new File(zkCfgFile).exists()) {
      LOG.error("zoo.cfg file doen't exist: " + zkCfgFile);
      System.exit(1);
    }

    String[] patterns = cmd.getOptionValues(pattern);

    String[] zkDataDirs = getZkDataDirs(zkCfgFile);

    // parse zk data files
    if (zkDataDirs == null || zkDataDirs[0] == null || zkDataDirs[1] == null) {
      LOG.error("invalid zkCfgDir: " + zkCfgDirValue);
      System.exit(1);
    }

    File zkParsedDir = new File(String.format("%s/zklog-parsed", System.getProperty("user.home")));
    if (!zkParsedDir.exists()) {
      LOG.info("creating zklog-parsed dir: " + zkParsedDir.getAbsolutePath());
      zkParsedDir.mkdir();
    }

    if (cmd.hasOption(between)) {
      String[] timeStrings = cmd.getOptionValues(between);

      long startTime = parseTimeString(timeStrings[0]);
      if (startTime == -1) {
        LOG.error("invalid start time string: " + timeStrings[0]
            + ", should be either timestamp or yyMMdd_hhmmss_SSS");
        System.exit(1);
      }

      long endTime = parseTimeString(timeStrings[1]);
      if (endTime == -1) {
        LOG.error("invalid end time string: " + timeStrings[1]
            + ", should be either timestamp or yyMMdd_hhmmss_SSS");
        System.exit(1);
      }

      if (startTime > endTime) {
        LOG.warn("empty window: " + startTime + " - " + endTime);
        System.exit(1);
      }
      // zkDataDirs[0] is the transaction log dir
      List<File> parsedZkLogs = parseZkLogs(zkDataDirs[0], startTime, endTime);
      grepZkLogDir(parsedZkLogs, startTime, endTime, patterns);

    } else if (cmd.hasOption(by)) {
      String timeString = cmd.getOptionValue(by);

      long byTime = parseTimeString(timeString);
      if (byTime == -1) {
        LOG.error("invalid by time string: " + timeString
            + ", should be either timestamp or yyMMdd_hhmmss_SSS");
        System.exit(1);
      }

      // zkDataDirs[1] is the snapshot dir
      File[] lastZkSnapshot = parseZkSnapshot(zkDataDirs[1], byTime);

      // lastZkSnapshot[1] is the parsed last snapshot by byTime
      grepZkSnapshot(lastZkSnapshot[1], patterns);

      // need to grep transaction logs between last-modified-time of snapshot and byTime also
      // lastZkSnapshot[0] is the last snapshot by byTime
      long startTime = lastZkSnapshot[0].lastModified();

      // zkDataDirs[0] is the transaction log dir
      List<File> parsedZkLogs = parseZkLogs(zkDataDirs[0], startTime, byTime);
      grepZkLogDir(parsedZkLogs, startTime, byTime, patterns);
    }
  }

  public static void main(String[] args) {
    processCommandLineArgs(args);
  }

}
