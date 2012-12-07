package org.apache.helix.filestore;

public class ChangeRecord
{
  /**
   * Transaction Id corresponding to the change that increases monotonically. 31
   * LSB correspond to sequence number that increments every change. 31 MSB
   * increments when the master changes
   */
  long txid;
  /**
   * File(s) that were changed
   */
  String file;

  /**
   * Timestamp
   */
  long timestamp;
  /**
   * Type of event like create, modified, deleted
   */
  short type;

  transient String changeLogFileName;

  transient long startOffset;

  transient long endOffset;
  
  public short fileFieldLength;

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(txid);
    sb.append("|");
    sb.append(timestamp);
    sb.append("|");
    sb.append(file);
    sb.append("|");
    sb.append(changeLogFileName);
    sb.append("|");
    sb.append(startOffset);
    sb.append("|");
    sb.append(endOffset);
    return sb.toString();
  }

  public static ChangeRecord fromString(String line)
  {
    ChangeRecord record=null;
    if (line != null)
    {
      String[] split = line.split("\\|");
      if (split.length == 6)
      {
        record = new ChangeRecord();
        record.txid = Long.parseLong(split[0]);
        record.timestamp = Long.parseLong(split[1]);
        record.file = split[2];
        record.changeLogFileName = split[3];
        record.startOffset = Long.parseLong(split[4]);
        record.endOffset = Long.parseLong(split[5]);
      }
    }
    return record;
  }

}
