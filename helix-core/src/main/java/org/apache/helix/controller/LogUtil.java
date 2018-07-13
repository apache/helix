package org.apache.helix.controller;

import org.slf4j.Logger;

public class LogUtil {
  public static void logInfo(Logger logger, String eventInfo, String message) {
    logger.info(String.format("Event %s : %s", eventInfo, message));
  }

  public static void logWarn(Logger logger, String eventInfo, String message) {
    logger.warn(String.format("Event %s : %s", eventInfo, message));
  }

  public static void logError(Logger logger, String eventInfo, String message) {
    logger.error(String.format("Event %s  %s", eventInfo, message));
  }

  public static void logDebug(Logger logger, String eventInfo, String message) {
    logger.debug(String.format("Event %s  %s", eventInfo, message));
  }

  public static void logInfo(Logger logger, String eventInfo, String message, Exception e) {
    logger.info(String.format("Event %s : %s", eventInfo, message), e);
  }

  public static void logWarn(Logger logger, String eventInfo, String message, Exception e) {
    logger.warn(String.format("Event %s : %s", eventInfo, message), e);
  }

  public static void logError(Logger logger, String eventInfo, String message, Exception e) {
    logger.error(String.format("Event %s  %s", eventInfo, message), e);
  }

  public static void logDebug(Logger logger, String eventInfo, String message, Exception e) {
    logger.debug(String.format("Event %s  %s", eventInfo, message), e);
  }
}
