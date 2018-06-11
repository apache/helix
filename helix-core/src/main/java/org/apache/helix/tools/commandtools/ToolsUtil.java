package org.apache.helix.tools.commandtools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static org.apache.helix.tools.commandtools.IntegrationTestUtil.*;

public class ToolsUtil {
  public static CommandLine processCommandLineArgs(String[] cliArgs, Options cliOptions) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }
}
