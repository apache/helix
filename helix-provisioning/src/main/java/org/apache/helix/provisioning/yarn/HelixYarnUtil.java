package org.apache.helix.provisioning.yarn;

import org.apache.log4j.Logger;

public class HelixYarnUtil {
  private static Logger LOG = Logger.getLogger(HelixYarnUtil.class);

  @SuppressWarnings("unchecked")
  public static <T extends ApplicationSpecFactory> T createInstance(String className) {
    Class<ApplicationSpecFactory> factoryClazz = null;
    {
      try {
        factoryClazz =
            (Class<ApplicationSpecFactory>) Thread.currentThread().getContextClassLoader()
                .loadClass(className);
      } catch (ClassNotFoundException e) {
        try {
          factoryClazz =
              (Class<ApplicationSpecFactory>) ClassLoader.getSystemClassLoader().loadClass(
                  className);
        } catch (ClassNotFoundException e1) {
          try {
            factoryClazz = (Class<ApplicationSpecFactory>) Class.forName(className);
          } catch (ClassNotFoundException e2) {

          }
        }
      }
    }
    System.out.println(System.getProperty("java.class.path"));
    if (factoryClazz == null) {
      LOG.error("Unable to find class:" + className);
    }
    ApplicationSpecFactory factory = null;
    try {
      factory = factoryClazz.newInstance();
    } catch (Exception e) {
      LOG.error("Unable to create instance of class: " + className, e);
    }
    return (T) factory;
  }
}
