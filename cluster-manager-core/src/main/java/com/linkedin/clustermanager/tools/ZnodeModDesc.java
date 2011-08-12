package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;

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
  
  public String _testName;
  public List<ZnodeModCommand> _commands = new ArrayList<ZnodeModCommand>();
  public List<ZnodeModVerifier> _verifiers = new ArrayList<ZnodeModVerifier>();
    
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
  
  // temp test
  public static void main(String[] args) 
  throws PropertyStoreException
  {
    String znodePath = "/TEST_CASES";
    ZnodeModDesc desc = new ZnodeModDesc("test1");
    
    // add commands
    desc.addCommand(new ZnodeModCommand(znodePath, 
                                        ZnodePropertyType.SIMPLE, 
                                        "+", 
                                        "key1",
                                        "value1"));
    
    desc.addCommand(new ZnodeModCommand(znodePath, 
                                        ZnodePropertyType.SIMPLE, 
                                        "+", 
                                        "key2",
                                        "value2_0",
                                        "value2_1"));
    
    
    // add verification
    desc.addVerification(new ZnodeModVerifier(znodePath, 
                                              ZnodePropertyType.SIMPLE, 
                                              "==", 
                                              "key1", 
                                              "value1"));
    
    desc.addVerification(new ZnodeModVerifier(znodePath, 
                                              ZnodePropertyType.SIMPLE, 
                                              "==", 
                                              "key2", 
                                              "value2_1"));
    
    PropertyJsonSerializer<ZnodeModDesc> serializer = new PropertyJsonSerializer<ZnodeModDesc>(ZnodeModDesc.class);
    byte[] bytes = serializer.serialize(desc);
    
    System.out.println(new String(bytes));
  }
  
}
