package org.apache.helix;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Helper class for Logging related stuff.
 */
public class LogHelper {

  /**
   * Updates the log level for the specified logger.
   * @param level Log level to update.
   * @param loggerName Name of the logger for which the level will be updated.
   */
  public static void updateLog4jLevel(org.apache.logging.log4j.Level level, String loggerName) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig =
        config.getLoggerConfig(String.valueOf(LogManager.getLogger(loggerName)));
    loggerConfig.setLevel(level);
    ctx.updateLoggers();
  }
}
