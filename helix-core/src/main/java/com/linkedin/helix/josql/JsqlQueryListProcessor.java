package com.linkedin.helix.josql;

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ZNRecord;

/**
 * Execute a list of combined queries. A combined query has the form {query}-->{resultName},
 * while the next query will have the previous result via the previous resultName.
 * 
 * */
public class JsqlQueryListProcessor
{
  public static final String SEPARATOR = "-->";
  private static Logger _logger = Logger.getLogger(JsqlQueryListProcessor.class);
  
  public static List<ZNRecord> executeQueryList(HelixManager manager, List<String> combinedQueryList) throws Exception
  {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ZNRecordQueryProcessor processor = new ZNRecordQueryProcessor();
    DataAccessorBasedTupleReader tupleReader = new DataAccessorBasedTupleReader(accessor, manager.getClusterName());
    List<ZNRecord> tempResult = null;
    for(int i = 0; i < combinedQueryList.size(); i++)
    {
      String combinedQuery = combinedQueryList.get(i);
      String query = combinedQuery;
      String resultName =  "";
      int pos = combinedQuery.indexOf(SEPARATOR);
      if(pos < 0)
      {
        if(i <combinedQueryList.size() - 1)
        {
          _logger.error("Combined query " + combinedQuery + " " + i +" doe not contain " + SEPARATOR);
        }
      }
      else
      {
        query = combinedQuery.substring(0, pos);
        resultName =  combinedQuery.substring(pos + SEPARATOR.length()).trim(); 
        if(resultName.length() == 0)
        {
          _logger.error("Combined query " + combinedQuery + " " + i + " doe not contain resultName");
        }
      }
      tempResult = processor.execute(query, tupleReader);
      if(resultName.length() > 0)
      {
        tupleReader.setTempTable(resultName, tempResult);
      }
    }
    
    return tempResult;
  }
}
