package com.linkedin.helix.josql;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.linkedin.helix.ZNRecord;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;

class OrderByVisitorImpl implements OrderByVisitor
{
  boolean isAsc = true;
  Expression expression = null;
  
  @Override
  public void visit(OrderByElement orderBy)
  {
    isAsc = orderBy.isAsc();
    expression = orderBy.getExpression();
  }
}

class FromItemVisitorImpl implements FromItemVisitor
{
  public Map<Integer, Table> tables = new HashMap<Integer, Table>();
  public Map<Integer, SelectVisitorImpl> subSelects = new HashMap<Integer, SelectVisitorImpl>();
  public List<Join> joins = new ArrayList<Join>();
  int fromItemIndex = 0;
  
  @Override
  public void visit(Table table)
  {
    tables.put(fromItemIndex++, table);
  }

  @Override
  public void visit(SubSelect subSelect)
  {
    SelectVisitorImpl sub = new SelectVisitorImpl();
    subSelect.getSelectBody().accept(sub);
    sub.alias = subSelect.getAlias();
    subSelects.put(fromItemIndex++, sub);
  }

  @Override
  public void visit(SubJoin subjoin)
  {
    throw new UnsupportedOperationException();
  }
}

class FunctionImpl
{
  String name;
  List<ExpressionVisitorImpl> expressionVisitors = new ArrayList<ExpressionVisitorImpl>();
}

class ExpressionVisitorImpl implements ExpressionVisitor
{
  Column column = null;
  FunctionImpl function = null;
  Object atomVal = null;
  EqualsTo equalsTo = null;
  AndExpression andExpression = null;
  OrExpression orExpression = null;
  MinorThan minorThan = null;
  GreaterThan greaterThan = null;
  Subtraction subtraction = null;
  Addition addition = null;
  Parenthesis parenthesis = null;
  
  Expression expression;
  
  ExpressionVisitorImpl(Expression expression)
  {
    this.expression = expression;
  }
  
  @Override
  public void visit(NullValue nullValue)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Function f)
  {
    function = new FunctionImpl();
    function.name = f.getName();
    ExpressionList parameters = f.getParameters();
    if(parameters != null)
    {
      @SuppressWarnings("unchecked")
      List<Expression> expressions = parameters.getExpressions();
      for(Expression e : expressions)
      {
        ExpressionVisitorImpl v = new ExpressionVisitorImpl(e);
        e.accept(v);
        function.expressionVisitors.add(v);
      }
    }
  }

  @Override
  public void visit(InverseExpression inverseExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(JdbcParameter jdbcParameter)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(DoubleValue doubleValue)
  {
    atomVal = Double.valueOf(doubleValue.getValue());
  }

  @Override
  public void visit(LongValue longValue)
  {
    atomVal = Long.valueOf(longValue.getValue());
  }

  @Override
  public void visit(DateValue dateValue)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(TimeValue timeValue)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(TimestampValue timestampValue)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Parenthesis parenthesis)
  {
    this.parenthesis = parenthesis;
  }

  @Override
  public void visit(StringValue stringValue)
  {
    atomVal = stringValue.getValue();
  }

  @Override
  public void visit(Addition addition)
  {
    this.addition = addition;
  }

  @Override
  public void visit(Division division)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Multiplication multiplication)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Subtraction subtraction)
  {
   this.subtraction = subtraction;
  }

  @Override
  public void visit(AndExpression andExpression)
  {
    this.andExpression = andExpression;
  }

  @Override
  public void visit(OrExpression orExpression)
  {
    this.orExpression = orExpression;
  }

  @Override
  public void visit(Between between)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(EqualsTo equalsTo)
  {
    this.equalsTo = equalsTo;
  }

  @Override
  public void visit(GreaterThan greaterThan)
  {
    this.greaterThan = greaterThan;
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(InExpression inExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(IsNullExpression isNullExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(LikeExpression likeExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(MinorThan minorThan)
  {
    this.minorThan = minorThan;
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Column tableColumn)
  {
    column = tableColumn;
  }

  @Override
  public void visit(SubSelect subSelect)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(CaseExpression caseExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(WhenClause whenClause)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(ExistsExpression existsExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(AllComparisonExpression allComparisonExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(AnyComparisonExpression anyComparisonExpression)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Concat concat)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(Matches matches)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(BitwiseAnd bitwiseAnd)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(BitwiseOr bitwiseOr)
  {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void visit(BitwiseXor bitwiseXor)
  {
    throw new UnsupportedOperationException(); 
  }
}

class SelectItemVisitorImpl implements SelectItemVisitor
{
  String alias;
  boolean shouldSelectAllCols = false;
  Expression expression = null;
  
  @Override
  public void visit(AllColumns allColumns)
  {
    shouldSelectAllCols = true;
  }

  @Override
  public void visit(AllTableColumns allTableColumns)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(SelectExpressionItem selectExpressionItem)
  {
    alias = selectExpressionItem.getAlias();
    expression = selectExpressionItem.getExpression();
  }
}

class SelectVisitorImpl implements SelectVisitor
{
  String alias = null;
  public List<SelectItemVisitorImpl> selectItemVisitors = new ArrayList<SelectItemVisitorImpl>();
  public FromItemVisitorImpl fromItemVisitor = new FromItemVisitorImpl();
  public List<OrderByVisitorImpl> orderByVisitors = null;
  public ExpressionVisitorImpl whereVisitor = null;
  public List<Column> groupByColumnReferences = null;
  public Union union = null;
  public Limit limit = null;
  
  @SuppressWarnings("unchecked")
  @Override
  public void visit(PlainSelect plainSelect)
  {
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    for(SelectItem selectItem : selectItems)
    {
      SelectItemVisitorImpl selectItemVisitor = new SelectItemVisitorImpl();
      selectItem.accept(selectItemVisitor);
      selectItemVisitors.add(selectItemVisitor);
    }
    
    plainSelect.getFromItem().accept(fromItemVisitor);
    if (plainSelect.getJoins() != null)
    {
      for (Iterator<Join> joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();)
      {
        Join join = joinsIt.next();
        if(join.getUsingColumns() == null || join.getUsingColumns().size() == 0)
        {
          throw new UnsupportedOperationException("'using' directive required for join");
        }
        if(join.getUsingColumns().size() != 2)
        {
          throw new UnsupportedOperationException("'using' directive for join requires exactly two columns");
        }
        join.getRightItem().accept(fromItemVisitor);
        fromItemVisitor.joins.add(join);
      }
    }

    Expression where = plainSelect.getWhere();
    if(where != null)
    {
      whereVisitor = new ExpressionVisitorImpl(where);
      where.accept(whereVisitor);
    }
    
    groupByColumnReferences = plainSelect.getGroupByColumnReferences();

    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    if(orderByElements != null)
    {
      orderByVisitors = new ArrayList<OrderByVisitorImpl>();
      for(OrderByElement e : orderByElements)
      {
        OrderByVisitorImpl impl = new OrderByVisitorImpl();
        e.accept(impl);
        orderByVisitors.add(impl);
      }
    }
    
    limit = plainSelect.getLimit();
  }

  @Override
  public void visit(Union union)
  {
    this.union = union;
  }
}

public class ZNRecordQueryProcessor
{
  public interface ZNRecordTupleReader
  {
    List<ZNRecord> get(String path) throws Exception;
    void reset();
  }

  private String replaceExplodeFields(String sql, String functionName)
  {
    String ret = sql;
    ret.replaceAll("`", ""); 
    
    Pattern pattern = Pattern.compile(functionName + "\\((.*?)\\)");
    Matcher m = pattern.matcher(ret);
    while (m.find())
    {
      String args[] = m.group(1).split(",");
      if(args.length  == 0) 
      { 
        throw new IllegalArgumentException(functionName + " requires at least 2 parameters"); 
      }
      
      String str = functionName + ".";
      String s = "";
      for (int i = 0; i < args.length; i++)
      {
        s += args[i].trim().replaceAll("`", "");
        if(i != args.length-1)
        {
          s += "_____";
        }
      }
      str += "`" + s + "`";

      ret = ret.replaceFirst(functionName + "\\(.*?\\)", str);
    }
    return ret;
  }
  
  public List<ZNRecord> execute(String sql, ZNRecordTupleReader tupleReader) throws Exception
  {
    sql = replaceExplodeFields(sql, "explodeList");
    sql = replaceExplodeFields(sql, "explodeMap");
    
    CCJSqlParserManager pm = new CCJSqlParserManager();
    Statement statement = pm.parse(new StringReader(sql));
    if (statement instanceof Select)
    {
      Select selectStatement = (Select) statement;
      SelectVisitorImpl v = new SelectVisitorImpl();
      selectStatement.getSelectBody().accept(v);
      return executeSelect(v, tupleReader);
    }
    
    throw new UnsupportedOperationException(statement.toString());
  }
  
  private void applyLimit(Limit limit, List<ZNRecord> input)
  {
    if(limit != null)
    {
      int rowCount = (int) limit.getRowCount();
      if(input != null && rowCount < input.size())
      {
        input.subList(rowCount, input.size()).clear();
      }
    }
  }
  
  private Integer compareRecords(ZNRecord o1, ZNRecord o2, ExpressionVisitorImpl expressionVisitor)
  {
    Object val1 = evalExpression(expressionVisitor, o1);
    Object val2 = evalExpression(expressionVisitor, o2);
    
    if(val1 == null || val2 == null) 
    {
      return null;
    }
    
    if(val1 instanceof Long && val2 instanceof Long)
    {
      return ((Long) val1).compareTo((Long)val2);
    }
    else if(val1 instanceof Double && val2 instanceof Double)
    {
      return ((Double)val1).compareTo((Double)val2);
    }
    else if(val1 instanceof String && val2 instanceof String)
    {
      return ((String)val1).compareTo((String) val2);
    }
    
    return null;
  }
  
  private void applyOrderBy(final List<OrderByVisitorImpl> orderByVisitors, final List<ZNRecord> result)
  {
    class RecordComparator implements Comparator<ZNRecord>
    {
      @Override
      public int compare(ZNRecord o1, ZNRecord o2)
      {
        for(OrderByVisitorImpl visitor : orderByVisitors)
        {
          ExpressionVisitorImpl expressionVisitor = new ExpressionVisitorImpl(visitor.expression);
          visitor.expression.accept(expressionVisitor);
          Integer compareResult = compareRecords(o1, o2, expressionVisitor);

          if(compareResult == null) 
          {
            return -1; //not comparable. return arbitrary "less" value
          }
          
          if(compareResult != 0)
          {
            return (visitor.isAsc ? compareResult : -compareResult);
          }
        }
        
        return 0;
      }
    }
    
    if(orderByVisitors != null)
    {
      Collections.sort(result, new RecordComparator());
    }
  }
  
  private List<ZNRecord> executePlainSelect(SelectVisitorImpl v, ZNRecordTupleReader tupleReader) throws Exception
  {
    SourceTableList sourceTables = getSourceTables(v, tupleReader);

    SourceTable joinResult = getJoin(v.fromItemVisitor.joins, sourceTables, 0, sourceTables.size()-1);
    List<ZNRecord> result = joinResult.tuples;
    
    //XXX: hack needed for now to allow aliased/computed columns to be used in where and group by
    //figure out a better way
    List<ZNRecord> tempProjection = getProjection(v, result);
    if(tempProjection != null)
    {
      for(int i = 0; i < result.size(); i++)
      {
        ZNRecord record = result.get(i);
        if(record instanceof ZNRecordSet)
        {
          ((ZNRecordSet)record).addRecord("xxx_random_"+System.currentTimeMillis(), tempProjection.get(i));
        }
        else
        {
          record.merge(tempProjection.get(i));
        }
      }
    }
    
    if(v.whereVisitor != null)
    {
      result = applyWhereClause(v, result);
    }

    if(v.groupByColumnReferences != null)
    {
      result = applyGroupBy(v, result);
    }
    else
    {
      ZNRecord aggregate = getAggregateColumns(v, result);
      if(aggregate != null)
      {
        result = Arrays.asList(aggregate);
      }
      else
      {
        List<ZNRecord> projection = getProjection(v, result);
        if(projection != null)
        {
          result = projection;
        }
        
        applyOrderBy(v.orderByVisitors, result);
        applyLimit(v.limit, result);
      }
    }
    
    return result;
  }
  
  private SourceTable getJoin(List<Join> joins,  SourceTableList sourceTables, int fromIndex, int toIndex)
  {
    assert(joins.size() == sourceTables.size()-1);
    if(fromIndex == toIndex)
    {
      return sourceTables.get(fromIndex);
    }
    else
    {
      SourceTable leftTable = sourceTables.get(fromIndex);
      List<Column> usingColumns = joins.get(fromIndex).getUsingColumns();
      Column leftCol = null;
      Column rightCol = null;
      if(usingColumns.get(0).getTable().getName().equals(leftTable.name))
      {
        leftCol = usingColumns.get(0);
        rightCol = usingColumns.get(1);
      }
      else
      {
        rightCol = usingColumns.get(0);
        leftCol = usingColumns.get(1);
      }
      SourceTable rightTable = getJoin(joins, sourceTables, fromIndex+1, toIndex);
      return getJoin(leftCol, rightCol, leftTable, rightTable);
    }
  }

  private SourceTable getJoin(Column leftCol,
                              Column rightCol,
                              SourceTable leftTable, 
                              SourceTable rightTable)
  {
    if(leftCol.getTable() == null || leftCol.getTable().equals(""))
    {
      throw new IllegalArgumentException(leftCol.toString() + " used in join should be fully qualified");
    }
    if(rightCol.getTable() == null || rightCol.getTable().equals(""))
    {
      throw new IllegalArgumentException(rightCol.toString() + " used in join should be fully qualified");
    }


    Map<String, Set<ZNRecord>> rightMap = new HashMap<String, Set<ZNRecord>>();
    for(ZNRecord record : rightTable.tuples)
    {
      String rightVal = getSimpleColumn(record, rightCol);
      if(rightVal != null)
      {
        if(!rightMap.containsKey(rightVal))
        {
          rightMap.put(rightVal, new HashSet<ZNRecord>());
        }
        rightMap.get(rightVal).add(record);
      }
    }
    
    List<ZNRecord> tuples = new ArrayList<ZNRecord>();
    for(ZNRecord leftRecord : leftTable.tuples)
    {
      String leftVal = getSimpleColumn(leftRecord, leftCol);
      if(leftVal != null && rightMap.containsKey(leftVal))
      {
        Set<ZNRecord> rightRecords = rightMap.get(leftVal);
        for(ZNRecord rightRecord : rightRecords)
        {
          if(rightRecord instanceof ZNRecordSet)
          {
            ((ZNRecordSet)rightRecord).addRecord(leftTable.name, leftRecord);
            tuples.add(rightRecord);
          }
          else
          {
            ZNRecordSet recordSet = new ZNRecordSet("");
            recordSet.addRecord(rightTable.name, rightRecord);
            recordSet.addRecord(leftTable.name, leftRecord);
            tuples.add(recordSet);
          }
        }
      }
    }

    return new SourceTable("xxx_random_" + System.currentTimeMillis(), 0, tuples);
  }

  private Integer evalComparisonExpression(BinaryExpression expression, ZNRecord record)
  {
    Object leftVal = evalExpression(expression.getLeftExpression(), record);
    Object rightVal = evalExpression(expression.getRightExpression(), record);
    if (leftVal != null && rightVal != null)
    {
      if (leftVal instanceof String && rightVal instanceof String)
      {
        return ((String) leftVal).compareTo((String) rightVal);
      }
      else if (leftVal instanceof Long || rightVal instanceof Long || 
          leftVal instanceof Double || rightVal instanceof Double)
      {
        Object l = toNum(leftVal);
        Object r = toNum(rightVal);
        if(l instanceof Long && r instanceof Long)
        {
          return ((Long) l).compareTo((Long) r);
        }
        else if (l instanceof Double && l instanceof Double)
        {
          return ((Double) l).compareTo((Double) r);
        }
      }
      else
      {
        throw new IllegalArgumentException("Left and right parameters of comparison cannot be compared");
      }
    }

    return null;
  }
  
  private Object evalExpression(ExpressionVisitorImpl visitor, ZNRecord record)
  {
    if(visitor.column != null)
    {
      return getSimpleColumn(record, visitor.column);
    }
    else if(visitor.function != null)
    {
      return evalFunction(record, visitor.function);
    }
    else if(visitor.atomVal != null)
    {
      return visitor.atomVal;
    }
    else if(visitor.equalsTo != null)
    {
      Integer ret = evalComparisonExpression(visitor.equalsTo, record);
      return Boolean.valueOf(ret != null && ret == 0);
    }
    else if(visitor.minorThan != null)
    {
      Integer ret = evalComparisonExpression(visitor.minorThan, record);
      return Boolean.valueOf(ret != null && ret < 0);
    }
    else if(visitor.greaterThan != null)
    {
      Integer ret = evalComparisonExpression(visitor.greaterThan, record);
      return Boolean.valueOf(ret != null && ret > 0);
    }
    else if(visitor.subtraction != null || visitor.addition != null)
    {
      Expression leftExpression = (visitor.subtraction != null ? visitor.subtraction.getLeftExpression() : visitor.addition.getLeftExpression());
      Expression rightExpression = (visitor.subtraction != null ? visitor.subtraction.getRightExpression() : visitor.addition.getRightExpression());
      Object leftVal = toNum(evalExpression(leftExpression, record));
      Object rightVal = toNum(evalExpression(rightExpression, record));
      if(leftVal != null && rightVal != null)
      {
        int multiplier = (visitor.subtraction != null ? -1 : 1);
        if (leftVal instanceof Long && rightVal instanceof Long)
        {
          return Long.valueOf((Long) leftVal + (multiplier * (Long) rightVal));
        }
        else if (leftVal instanceof Double && rightVal instanceof Double)
        {
          return Double.valueOf((Double) leftVal + (multiplier * (Double) rightVal));
        }
      }
    }
    else if(visitor.andExpression != null)
    {
      Object leftVal = evalExpression(visitor.andExpression.getLeftExpression(), record);
      Object rightVal = evalExpression(visitor.andExpression.getRightExpression(), record);
      if(leftVal != null && rightVal != null)
      {
        if(!(leftVal instanceof Boolean) || !(rightVal instanceof Boolean))
        {
          throw new IllegalArgumentException("AND clause parameters don't evaluate to boolean");
        }
        return Boolean.valueOf(((Boolean)leftVal) && ((Boolean)rightVal));
      }
      else
      {
        return Boolean.FALSE;
      }
    }
    else if(visitor.parenthesis != null)
    {
      return evalExpression(visitor.parenthesis.getExpression(), record);
    }
    
    return null;
  }
  
  private Object evalExpression(Expression exp, ZNRecord record)
  {
    //TODO: Replace logic here with generic expression evaluation
    ExpressionVisitorImpl visitor = new ExpressionVisitorImpl(exp);
    exp.accept(visitor);
    return evalExpression(visitor, record);
  }
  
  private List<ZNRecord> applyWhereClause(SelectVisitorImpl v,
                                          List<ZNRecord> input)
  {
    List<ZNRecord> result = new ArrayList<ZNRecord>();
    for (ZNRecord record : input)
    {
      Object evalResult = evalExpression(v.whereVisitor, record);
      if(evalResult == null || !(evalResult instanceof Boolean))
      {
        throw new UnsupportedOperationException("Unsupported predicate in where clause:" + v.whereVisitor.expression);
      }
      else if((Boolean)evalResult)
      {
        result.add(record);
      }
    }

    return result;
  }

  private List<ZNRecord> executeUnion(SelectVisitorImpl v, ZNRecordTupleReader tupleReader) throws Exception
  {
    @SuppressWarnings("unchecked")
    List<PlainSelect> plainSelects = v.union.getPlainSelects();
    List<ZNRecord> result = new ArrayList<ZNRecord>();
    for(PlainSelect select : plainSelects)
    {
      SelectVisitorImpl visitor = new SelectVisitorImpl();
      select.accept(visitor);
      result.addAll(executePlainSelect(visitor, tupleReader));
    }
    
    applyLimit(v.limit, result);
    return result;
  }
  
  private List<ZNRecord> executeSelect(SelectVisitorImpl v, ZNRecordTupleReader tupleReader) throws Exception
  {
    if(v.union != null)
    {
      return executeUnion(v, tupleReader);
    }
    else
    {
      return executePlainSelect(v, tupleReader);
    }
  }

  private String getSimpleColumn(ZNRecord record, Column column)
  {
    if(record == null)
    {
      return null;
    }
    
    String ret = record.getSimpleField(column.getWholeColumnName());
    if(ret == null)
    {
      ret = record.getSimpleField(column.getColumnName());
    }
    if(ret == null)
    {
      Map<String, String> simpleFields = record.getSimpleFields();
      for(String key : simpleFields.keySet())
      {
        if(key.endsWith("." + column.getColumnName()))
        {
          ret = simpleFields.get(key);
        }
      }
    }
    return ret;
  }
  
  private Map<Map<Column, String>, List<ZNRecord>> formGroups(List<Column> groupByCols, List<ZNRecord> input)
  {    
    Map<Map<Column, String>, List<ZNRecord>> map = new HashMap<Map<Column,String>, List<ZNRecord>>();
    for(ZNRecord record : input)
    {
      Map<Column, String> group = new HashMap<Column, String>();
      for(Column col : groupByCols)
      {
        String val = getSimpleColumn(record, col);
        if(val != null)
        {
          group.put(col, val);
        }
      }
      if(group.size() != 0)
      {
        if(!map.containsKey(group))
        {
          map.put(group, new ArrayList<ZNRecord>());
        }
        map.get(group).add(record);
      }
    }
    return map;
  }
  
  //TODO: Validate that only group by keys and aggregate functions are in select items
  private List<ZNRecord> applyGroupBy(SelectVisitorImpl v, List<ZNRecord> input) throws Exception
  {
    List<ZNRecord> result = new ArrayList<ZNRecord>();
    Map<Map<Column, String>, List<ZNRecord>> groups = formGroups(v.groupByColumnReferences, input);
    for(Map<Column, String> group : groups.keySet())
    {
      List<ZNRecord> tuples = groups.get(group);
      ZNRecord aggregate = getAggregateColumns(v, tuples);
      if(aggregate != null)
      {
        for(Column col : group.keySet())
        {
          String val = group.get(col);
          aggregate.setSimpleField(col.getWholeColumnName(), val);
        }
        result.add(aggregate);
      }
    }
    
    return result;
  }
  
  //TODO: Generalize this
  private ZNRecord getAggregateColumns(SelectVisitorImpl v, List<ZNRecord> input) throws Exception
  {
    for(SelectItemVisitorImpl impl : v.selectItemVisitors)
    {
      if(impl.expression != null)
      {
        ExpressionVisitorImpl expressionVisitor = new ExpressionVisitorImpl(impl.expression); 
        impl.expression.accept(expressionVisitor);
        if(expressionVisitor.function != null && (expressionVisitor.function.name.equalsIgnoreCase("max") 
            || expressionVisitor.function.name.equalsIgnoreCase("min")))
        {
          if (expressionVisitor.function.expressionVisitors.size() != 1)
          {
            throw new IllegalArgumentException(expressionVisitor.function.name + " needs one argument");
          }
          
          ZNRecord min = null;
          ZNRecord max = null;
          for(ZNRecord record : input)
          {
            if(min == null || compareRecords(record, min, expressionVisitor.function.expressionVisitors.get(0)) < 0)
            {
              min = record;
            }
            if(max == null || compareRecords(record, max, expressionVisitor.function.expressionVisitors.get(0)) > 0)
            {
              max = record;
            }
          }
          
          Object val = null;
          if(expressionVisitor.function.name.equalsIgnoreCase("max"))
          {
           val = evalExpression(expressionVisitor.function.expressionVisitors.get(0), max); 
          }
          else
          {
            val = evalExpression(expressionVisitor.function.expressionVisitors.get(0), min);
          }
          ZNRecord ret = new ZNRecord("");
          String columnName = (impl.alias != null ? impl.alias : expressionVisitor.function.name);
          if(val != null)
          {
            ret.setSimpleField(columnName, ""+val);
          }
          return ret;
        }
      }
    }
    return null;
  }

  //return null if all columns are projected
  private  List<ZNRecord> getProjection(SelectVisitorImpl v, List<ZNRecord> input) throws Exception
  {
    List<ZNRecord> output = new ArrayList<ZNRecord>();
    
    for (ZNRecord record : input)
    {
      ZNRecord projection = new ZNRecord("");
      for(SelectItemVisitorImpl impl : v.selectItemVisitors)
      {
        if(impl.shouldSelectAllCols)
        {
          return null;
        }
        else if(impl.expression != null)
        {
          ExpressionVisitorImpl expressionVisitor = new ExpressionVisitorImpl(impl.expression); 
          impl.expression.accept(expressionVisitor);
          String columnName = impl.alias;
          Object val = evalExpression(expressionVisitor, record);
          if(expressionVisitor.column != null && columnName == null)
          {
            columnName = expressionVisitor.column.getWholeColumnName();
          }
          else if(expressionVisitor.function != null && columnName == null)
          {
            columnName = expressionVisitor.function.name;
          }
          else if(columnName == null)
          {
            columnName = impl.expression.toString();
          }
          if(val != null)
          {
            projection.setSimpleField(columnName, val.toString());
          }
        }
      }

      output.add(projection);
    }
    return output;
  }

  private Object getParamValue(FunctionImpl function, int index, ZNRecord record)
  {
    if(function.expressionVisitors.get(index).function != null)
    {
      return evalFunction(record, function.expressionVisitors.get(index).function);
    }
    else if(function.expressionVisitors.get(index).column != null)
    {
      return getSimpleColumn(record, function.expressionVisitors.get(index).column);
    }
    else
    {
      return function.expressionVisitors.get(index).atomVal;
    }
  }
  
  private Object evalFunction(ZNRecord record, FunctionImpl function)
  {
    if(function.name.equalsIgnoreCase("concat"))
    {
      if (function.expressionVisitors.size() != 2)
      {
        throw new IllegalArgumentException("concat() needs 2 arguments");
      }
      Object arg1 = getParamValue(function, 0, record);
      Object arg2 = getParamValue(function, 1, record);
      String ret = (arg1 != null ? arg1.toString() : "");
      ret += (arg2 != null ? arg2.toString() : "");
      return ret;
    }
    else if(function.name.equalsIgnoreCase("to_number"))
    {
      if (function.expressionVisitors.size() != 1)
      {
        throw new IllegalArgumentException("to_number() needs 1 argument");
      }
      Object param = getParamValue(function, 0, record);
      return toNum(param);
    }

    return null;
  }

  private Object toNum(Object o)
  {
    if(o == null)
    {
      return null;
    }
    
    if(o instanceof String)
    {
      String s = (String) o;
      if(s.contains("."))
      {
        try {return Double.parseDouble(s);}catch(Exception e){}
      }
      else
      {
        try {return Long.parseLong(s);}catch(Exception e){}
      }
    }
    else if(o instanceof Long || o instanceof Double)
    {
      return o;
    }
    
    return null;
  }
 
  private SourceTableList getSourceTables(SelectVisitorImpl v, ZNRecordTupleReader tupleReader) throws Exception
  {
    SourceTableList sourceTables = new SourceTableList();
    
    for(int fromItemIndex : v.fromItemVisitor.tables.keySet())
    {
      Table t = v.fromItemVisitor.tables.get(fromItemIndex);
      String schema = t.getSchemaName();
      if(schema != null)
      {
        schema = schema.replaceAll("`", "");
      }
      String tableName = t.getName();
      tableName = tableName.replaceAll("`", "");
      List<ZNRecord> tuples = null;
      String key = null;
      if(schema != null && (schema.equalsIgnoreCase("explodeList") || schema.equalsIgnoreCase("explodeMap")))
      {
        tuples = getExplodedTable(schema, tableName, tupleReader);
        key = (t.getAlias() != null ? t.getAlias() : "xxx_random_" + System.currentTimeMillis());
      }
      else
      {
        tuples = tupleReader.get(tableName);
        key = (t.getAlias() != null ? t.getAlias() : tableName);
      }
      sourceTables.add(new SourceTable(key, fromItemIndex, tuples));
    }
    
    for(int fromItemIndex : v.fromItemVisitor.subSelects.keySet())
    {
      SelectVisitorImpl subSelect = v.fromItemVisitor.subSelects.get(fromItemIndex);
      List<ZNRecord> tuples = executeSelect(subSelect, tupleReader);
      String key = (subSelect.alias != null ? subSelect.alias : "xxx_random_" + System.currentTimeMillis());
      sourceTables.add(new SourceTable(key, fromItemIndex, tuples));
    }
    
    //XXX: Special case of ID column
    for(SourceTable table : sourceTables)
    {
      for(ZNRecord record : table.tuples)
      {
        if(!record.getSimpleFields().containsKey("id"))
        {
          record.setSimpleField("id", record.getId());
        }
      }
    }
    
    return sourceTables;
  }

  private List<ZNRecord> getExplodedTable(String function,
                                          String arg,
                                          ZNRecordTupleReader tupleReader) throws Exception
  {
    List<String> args = Arrays.asList(arg.split("_____"));
    String originalTable = args.get(0);
    List<String> columns = (args.size() > 1 ? args.subList(1, args.size()) : null);
    if(function.equalsIgnoreCase("explodeList"))
    {
      return explodeListFields(originalTable, columns, tupleReader);
    }
    else if(function.equalsIgnoreCase("explodeMap"))
    {
      return explodeMapFields(originalTable, columns, tupleReader);
    }
    
    throw new IllegalArgumentException(function + " is not supported");
  }

  private List<ZNRecord> explodeMapFields(String tableName,
                                          Collection<String> mapColumns,
                                          ZNRecordTupleReader tupleReader) throws Exception
  {
    List<ZNRecord> table = tupleReader.get(tableName);
    List<ZNRecord> ret = new ArrayList<ZNRecord>();
    for(ZNRecord record : table)
    {
      Collection<String> cols = (mapColumns == null ? record.getMapFields().keySet() : mapColumns);
 
      for(String mapCol : cols) //for each map field
      {
        if(record.getMapFields().containsKey(mapCol))
        {
          ZNRecord newRecord = new ZNRecord("");
          newRecord.getSimpleFields().putAll(record.getSimpleFields());
          newRecord.setSimpleField("mapField", mapCol);
          Map<String, String> mapField = record.getMapField(mapCol);
          for(String key : mapField.keySet())
          {
            newRecord.setSimpleField(key, mapField.get(key));
          }
          ret.add(newRecord);
        }
      }
    }
    
    return ret;
  }

  private List<ZNRecord> explodeListFields(String tableName,
                                           Collection<String> listColumns,
                                           ZNRecordTupleReader tupleReader) throws Exception
  {
    List<ZNRecord> table = tupleReader.get(tableName);
    List<ZNRecord> ret = new ArrayList<ZNRecord>();
    for(ZNRecord record : table)
    {
      Collection<String> cols = (listColumns == null ? record.getListFields().keySet() : listColumns);
      for(String listCol : cols) //for each list field
      {
        if(record.getListFields().containsKey(listCol))
        {
          List<String> listField = record.getListField(listCol);
          for(int listIndex = 0; listIndex < listField.size(); listIndex++)
          {
            String val = listField.get(listIndex);
            ZNRecord newRecord = new ZNRecord("");
            newRecord.getSimpleFields().putAll(record.getSimpleFields());
            newRecord.setSimpleField("listField", listCol);
            newRecord.setSimpleField("listIndex", ""+listIndex);
            newRecord.setSimpleField("listVal", val);
            ret.add(newRecord);
          }
        }
      }
    }
    return ret;
  }
}

class ZNRecordSet extends ZNRecord
{
  private HashMap<String, ZNRecord> _records = new HashMap<String, ZNRecord>();

  public ZNRecordSet(String id)
  {
    super(id);
    // TODO Auto-generated constructor stub
  }
  
  public void addRecord(String tableName, ZNRecord record)
  {
    _records.put(tableName, record);
  }
  
  @Override
  public String getSimpleField(String key)
  {
    if(key.contains("."))
    {
      String tmp[] = key.split("\\.");
      if(_records.containsKey(tmp[0]))
      {
        return _records.get(tmp[0]).getSimpleField(tmp[1]);
      }
    }
    else
    {
      for(ZNRecord record : _records.values())
      {
        if(record.getSimpleFields().containsKey(key))
        {
          return record.getSimpleField(key);
        }
      }
    }
    
    return null;
  }
  
  @Override
  public void setSimpleField(String key, String val)
  {
    if(key.contains("."))
    {
      String tmp[] = key.split("\\.");
      if(_records.containsKey(tmp[0]))
      {
        _records.get(tmp[0]).setSimpleField(tmp[1], val);
      }
      else
      {
        ZNRecord record = new ZNRecord("");
        record.setSimpleField(tmp[1], val);
        addRecord(tmp[0], record);
      }
    }
    else
    {
      if(_records.size() == 0)
      {
        ZNRecord record = new ZNRecord("");
        record.setSimpleField(key, val);
        addRecord("xxx_random_" + System.currentTimeMillis(), record);
      }
      else
      {
        _records.get(_records.keySet().iterator().next()).setSimpleField(key, val);
      }
    }
  }
  
  @Override
  public String toString()
  {
    return "@" + _records.toString() + "@";
  }
}

class SourceTableList implements Iterable<SourceTable>
{
  Map<String, SourceTable> _nameMap = new HashMap<String, SourceTable>();
  Map<Integer, SourceTable> _indexMap = new HashMap<Integer, SourceTable>();
  
  public void add(SourceTable table)
  {
    _nameMap.put(table.name, table);
    _indexMap.put(table.fromItemIndex, table);
  }
  
  public int size()
  {
    return _nameMap.size();
  }
  
  public SourceTable get(int index)
  {
    if(index >= _indexMap.size())
    {
      throw new IndexOutOfBoundsException();
    }
    return _indexMap.get(index);
  }
  
  public SourceTable get(String name)
  {
    return _nameMap.get(name);
  }
  
  public boolean containsKey(String name)
  {
    return _nameMap.containsKey(name);
  }
  
  public Iterator<SourceTable> iterator()
  {
    return _nameMap.values().iterator();
  }
  
  @Override
  public String toString()
  {
    return _nameMap.toString();
  }
}

class SourceTable
{
  String name;
  int fromItemIndex;
  List<ZNRecord> tuples;
  
  public SourceTable(String name, int fromItemIndex, List<ZNRecord> tuples)
  {
    this.name = name;
    this.fromItemIndex = fromItemIndex;
    this.tuples = tuples;
  }
  
  @Override
  public String toString()
  {
    return name + "->" + tuples;
  }
}