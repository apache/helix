package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.thoughtworks.xstream.XStream;

/*
 * a test case is structured logically as:
 * <test-case name = "test1">
 *   <commands>
 *     <trigger1 = (start-time, timeout, expect-value), <--optional 
 *      command1 = znode-path, 
 *                 property-type(s, m, or l), 
 *                 op(+,-), 
 *                 key(s->k1; l->k1, k1|index; m->k1, k1|k2),
 *                 update_value(optional) >
 *     <command2 = ... >
 *   </commands>
 *     
 *   <verifiers>
 *     <verifier1 = timeout,
 *                  znode-path,
 *                  property-type(s, m, or l),
 *                  op(==, !=),
 *                  key(s->k1; l->k1, k1|index; m->k1, k1|k2,
 *                  value >
 *     <verfier2 = ... >    
 *   </verifiers>
 * </test-case>
 */

public class ZnodeModDesc
{

  public enum ZnodePropertyType
  {
    SIMPLE,   // simple field
    LIST,     // list field
    MAP       // map field
  }
  
  private String _testName;
  private List<ZnodeModCommand> _commands = new ArrayList<ZnodeModCommand>();
  private List<ZnodeModVerifier> _verifiers = new ArrayList<ZnodeModVerifier>();
    
  public ZnodeModDesc(String testName)
  {
    _testName = testName;
  }
  
  public void addCommand(ZnodeModCommand command)
  {
    if (command != null)
    {
      _commands.add(command);
    }
  }
  
  public void addVerification(ZnodeModVerifier verification)
  {
    if (verification != null)
    {
      _verifiers.add(verification);
    }
  }
  
  // getter/setter's
  public void setTestName(String testName)
  {
    _testName = testName;
  }
  
  public String getTestName()
  {
    return _testName;
  }
  
  public void setCommands(List<ZnodeModCommand> commands)
  {
    _commands = commands;
  }
  
  public List<ZnodeModCommand> getCommands()
  {
    return _commands;
  }
  
  public void setVerfiers(List<ZnodeModVerifier> verifiers)
  {
    _verifiers = verifiers;
  }
  
  public List<ZnodeModVerifier> getVerifiers()
  {
    return _verifiers;
  }
  
  // temp test
  public static void main(String[] args) 
  throws PropertyStoreException
  {
    String znodePath = "/testPath";
    ZnodeModDesc desc = new ZnodeModDesc("test1");
    
    // add commands
    desc.addCommand(new ZnodeModCommand(znodePath, 
                                        ZnodePropertyType.SIMPLE, 
                                        "+", 
                                        "key1",
                                        new ZnodeModValue("value1")));
    
    List<String> expectList = new ArrayList<String>();
    expectList.add("value1");
    expectList.add("value2");
    
    List<String> updateList = new ArrayList<String>();
    updateList.add("value1_new");
    updateList.add("value2_new");
    
    desc.addCommand(new ZnodeModCommand(znodePath, 
                                        ZnodePropertyType.LIST, 
                                        "+", 
                                        "key2",
                                        new ZnodeModValue(expectList),
                                        new ZnodeModValue(updateList)));
    
    
    // add verification
    desc.addVerification(new ZnodeModVerifier(znodePath, 
                                              ZnodePropertyType.SIMPLE, 
                                              "==", 
                                              "key1", 
                                              new ZnodeModValue("value1")));
    List<String> expectList2 = new ArrayList<String>();
    expectList2.add("value1_new");
    expectList2.add("value2_new");
    desc.addVerification(new ZnodeModVerifier(znodePath, 
                                              ZnodePropertyType.LIST, 
                                              "==", 
                                              "key2", 
                                              new ZnodeModValue(expectList2)));
    
    PropertyJsonSerializer<ZnodeModDesc> serializer = new PropertyJsonSerializer<ZnodeModDesc>(ZnodeModDesc.class);
    byte[] bytes = serializer.serialize(desc);
    System.out.println(new String(bytes));
    
    XStream xStream = new XStream();
    System.out.println(xStream.toXML(desc));
  }
  
}
