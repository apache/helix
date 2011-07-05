package com.linkedin.dds.test.infra;

import java.io.IOException;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * This class is a helper to connect to our JMX agent that allows access to all the MBeans in all
 * the containers running on a given server.
 * <p>
 * To use this class:
 * <ul>
 * <li>Use one of the {@code create} methods to create an instance.
 * <li>Use one of the {@code getMBeanProxy} methods to get a handle to the MBean(s) that you
 * want to interact with (i.e., that you want to get/set MBean attributes or invoke MBean operations).
 * <li>When done with the instance, call {@code close}.
 * </ul>
 * 
 * @author Mitch Stuart
 * @version $Revision: 69375 $
 */
public class JMXAgentHelper
{
  public static final String JMX_AGENT_URL_STRING_PREFIX = "service:jmx:rmi:///jndi/rmi://";
  public static final String JMX_AGENT_URL_STRING_PREFIX_1 = "service:jmx:rmi://localhost:";
  public static final String JMX_AGENT_URL_STRING_PREFIX_2 = "/jndi/rmi://";
  public static final String JMX_AGENT_URL_STRING_SUFFIX = "/jmxrmi";
  public static final String JMX_AGENT_DEFAULT_HOST = "localhost";
  public static final int JMX_AGENT_DEFAULT_PORT = 12000;

  /**
   * The URL string needed to connect to the JMX agent on the default host and port
   * <p>
   * For example: {@code service:jmx:rmi:///jndi/rmi://localhost:12000/jmxrmi}
   */
  public static final String JMX_AGENT_DEFAULT_URL_STRING = makeJMXAgentURLString(JMX_AGENT_DEFAULT_HOST, JMX_AGENT_DEFAULT_PORT);
  
  
  /**
   * Create the URL string needed to connect to the JMX agent
   * <p>
   * If you are using the default host and port, just use {@link #JMX_AGENT_DEFAULT_URL_STRING}
   * instead of calling this method.
   */
  public static String makeJMXAgentURLString(String host, int port)
  {
    return JMX_AGENT_URL_STRING_PREFIX +
      host +
      ":" +
      port +
      JMX_AGENT_URL_STRING_SUFFIX;
  }
  
  /**
   * Create the URL string needed to connect to the JMX agent
   * <p>
   * If you are using the default host and port, just use {@link #JMX_AGENT_DEFAULT_URL_STRING}
   * instead of calling this method.
   */
  public static String makeJMXAgentURLString(String host, int port, int surffix)
  {
    return JMX_AGENT_URL_STRING_PREFIX_1 +
      surffix +
      JMX_AGENT_URL_STRING_PREFIX_2 +
      host +
      ":" +
      port +
      JMX_AGENT_URL_STRING_SUFFIX +
      surffix;
  }
  

  /**
   * Create an instance using {@link #JMX_AGENT_DEFAULT_URL_STRING} to connect to the agent
   * <p>
   * Remember to call {@link #close()} when you are done with this instance
   */
  public static JMXAgentHelper create()
    throws IOException
  {
    return create(JMX_AGENT_DEFAULT_URL_STRING);
  }
  
  /**
   * Create an instance using the specified URL string to connect to the agent
   * <p>
   * Remember to call {@link #close()} when you are done with this instance
   * <p>
   * You can use {@link #makeJMXAgentURLString(String, int)} to create the URL string
   * to pass to this method. If you want to connect to the agent on the default
   * host/port, you can just use {@link #create()} instead of this method.
   */
  public static JMXAgentHelper create(String jmxAgentURLString)
    throws IOException
  {
    JMXServiceURL jmxUrl = new JMXServiceURL(jmxAgentURLString);
    JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl);
    MBeanServerConnection mbeanServerConn = jmxConnector.getMBeanServerConnection();
    
    JMXAgentHelper jmxAgentHelper = new JMXAgentHelper(jmxConnector, mbeanServerConn);
    return jmxAgentHelper;
  }
  
  
  
  private final JMXConnector _jmxConnector;
  private final MBeanServerConnection _mbeanServerConn;

  /**
   * Private constructor - use one of the create static methods to get an instance
   */
  private JMXAgentHelper(JMXConnector jmxConnector,
                         MBeanServerConnection mbeanServerConn)
  {
    _jmxConnector = jmxConnector;
    _mbeanServerConn = mbeanServerConn;
  }

  /**
   * Create and return a proxy to the requested MBean. Using this proxy you can get and set
   * the MBean's attributes and invoke its operations.
   * 
   * @param containerPrefix the container prefix indicating to the agent which container
   * the MBean is hosted in. For example {@code backend-container}.
   * @param mbeanObjectNameString the MBean name <b>not</b> including the container
   * prefix, for example: {@code member2rep.member2rep:type=DatabusAdmin,name=memrepDalAdmin}
   * @param mbeanInterfaceType the {@code Class} of the MBean's interface
   * @return a proxy to the MBean
   * @throws MalformedObjectNameException
   */
  @SuppressWarnings("unchecked")
  public <T> T getMBeanProxy(String containerPrefix, String mbeanObjectNameString, Class mbeanInterfaceType)
    throws MalformedObjectNameException
  {
    String fullObjectNameString = containerPrefix + "/" + mbeanObjectNameString;
    T mbeanProxy = (T) getMBeanProxy(fullObjectNameString, mbeanInterfaceType);
    return mbeanProxy;
  }
  
  /**
   * Create and return a proxy to the requested MBean. Using this proxy you can get and set
   * the MBean's attributes and invoke its operations.
   * 
   * @param mbeanObjectNameString the fully-qualified MBean name including the container
   * prefix, for example: {@code backend-container/member2rep.member2rep:type=DatabusAdmin,name=memrepDalAdmin}
   * @param mbeanInterfaceType the {@code Class} of the MBean's interface
   * @return a proxy to the MBean
   * @throws MalformedObjectNameException
   */
  @SuppressWarnings("unchecked")
  public <T> T getMBeanProxy(String mbeanObjectNameString, Class mbeanInterfaceType)
    throws MalformedObjectNameException
  {
    T mbeanProxy = (T) MBeanServerInvocationHandler.newProxyInstance(
      _mbeanServerConn,
      new ObjectName(mbeanObjectNameString),
      mbeanInterfaceType,
      false);
    
    return mbeanProxy;
  }
  
  public Set<ObjectName> getMBeanNames() throws IOException
  {
    return _mbeanServerConn.queryNames(null, null);
  }
  
  public Set<ObjectInstance> getMBeans() throws IOException
  {
    return _mbeanServerConn.queryMBeans(null, null);
  }
  
  public void close()
    throws IOException
  {
    _jmxConnector.close();
  }

}
