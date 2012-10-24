package org.apache.helix;

import java.util.List;

public class ZNRecordAssembler
{
  public ZNRecord assemble(List<ZNRecord> records)
  {
    ZNRecord assembledRecord = null;
    if (records != null && records.size() > 0)
    {
      for (ZNRecord record : records)
      {
        if (record == null)
        {
          continue;
        }

        if (assembledRecord == null)
        {
          assembledRecord = new ZNRecord(record.getId());
        }

        assembledRecord.merge(record);
      }
    }
    return assembledRecord;
  }

}
