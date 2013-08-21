package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * Wrapper for running commands outside of the JVM
 * @see {@link Process}
 */
public class ExternalCommand {
  public static final String MODULE = ExternalCommand.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final ProcessBuilder _processBuilder;

  private Process _process;
  private InputReader _out;
  private InputReader _err;

  /**
   * Stream redirector
   */
  private static class InputReader extends Thread {
    private static final int BUFFER_SIZE = 2048;

    private final InputStream _in;
    private final ByteArrayOutputStream _out;
    private boolean _running = false;

    InputReader(InputStream in) {
      _in = in;
      _out = new ByteArrayOutputStream();
    }

    @Override
    public void run() {
      _running = true;

      byte[] buf = new byte[BUFFER_SIZE];
      int n = 0;
      try {
        while ((n = _in.read(buf)) != -1)
          _out.write(buf, 0, n);
      } catch (IOException e) {
        LOG.error("error while reading external command", e);
      }

      _running = false;
    }

    public byte[] getOutput() {
      if (_running)
        throw new IllegalStateException("wait for process to be completed");

      return _out.toByteArray();
    }
  }

  /**
   * Initialize with a {@link ProcessBuilder}
   * @param processBuilder initialized {@link ProcessBuilder} object
   */
  public ExternalCommand(ProcessBuilder processBuilder) {
    _processBuilder = processBuilder;
  }

  /**
   * After creating the command, you have to start it...
   * @throws IOException
   */
  public void start() throws IOException {
    _process = _processBuilder.start();
    _out = new InputReader(new BufferedInputStream(_process.getInputStream()));
    _err = new InputReader(new BufferedInputStream(_process.getErrorStream()));

    _out.start();
    _err.start();
  }

  /**
   * @see {@link ProcessBuilder#environment()}
   */
  public Map<String, String> getEnvironment() {
    return _processBuilder.environment();
  }

  /**
   * @see {@link ProcessBuilder#directory()}
   */
  public File getWorkingDirectory() {
    return _processBuilder.directory();
  }

  /**
   * @see {@link ProcessBuilder#directory(File)}
   */
  public void setWorkingDirectory(File directory) {
    _processBuilder.directory(directory);
  }

  /**
   * @see {@link ProcessBuilder#redirectErrorStream()}
   */
  public boolean getRedirectErrorStream() {
    return _processBuilder.redirectErrorStream();
  }

  /**
   * @see {@link ProcessBuilder#redirectErrorStream(boolean)}
   */
  public void setRedirectErrorStream(boolean redirectErrorStream) {
    _processBuilder.redirectErrorStream(redirectErrorStream);
  }

  /**
   * Get the contents of the output stream after completion
   * @return bytes from the output stream
   * @throws InterruptedException the process was interrupted before completion
   */
  public byte[] getOutput() throws InterruptedException {
    waitFor();
    return _out.getOutput();
  }

  /**
   * Get the contents of the error stream after completion
   * @return bytes from the error stream
   * @throws InterruptedException the process was interrupted before completion
   */
  public byte[] getError() throws InterruptedException {
    waitFor();
    return _err.getOutput();
  }

  /**
   * Returns the output as a string.
   * @param encoding string encoding scheme, e.g. "UTF-8"
   * @return encoded string
   * @throws InterruptedException the process was interrupted before completion
   * @throws UnsupportedEncodingException the encoding scheme is invalid
   */
  public String getStringOutput(String encoding) throws InterruptedException,
      UnsupportedEncodingException {
    return new String(getOutput(), encoding);
  }

  /**
   * Returns the output as a string. Uses encoding "UTF-8".
   * @return utf8 encoded string
   * @throws InterruptedException the process was interrupted before completion
   */
  public String getStringOutput() throws InterruptedException {
    try {
      return getStringOutput("UTF-8");
    } catch (UnsupportedEncodingException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the error as a string.
   * @param encoding the encoding scheme, e.g. "UTF-8"
   * @return error as string
   * @throws InterruptedException the process was interrupted before completion
   * @throws UnsupportedEncodingException the encoding scheme is invalid
   */
  public String getStringError(String encoding) throws InterruptedException,
      UnsupportedEncodingException {
    return new String(getError(), encoding);
  }

  /**
   * Returns the error as a string. Uses encoding "UTF-8".
   * @return error as string
   * @throws InterruptedException the process was interrupted before completion
   */
  public String getStringError() throws InterruptedException {
    try {
      return getStringError("UTF-8");
    } catch (UnsupportedEncodingException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }

  /**
   * Properly waits until everything is complete: joins on the thread that
   * reads the output, joins on the thread that reads the error and finally
   * wait for the process to be finished.
   * @return the status code of the process.
   * @throws InterruptedException the process was interrupted before completion
   */
  public int waitFor() throws InterruptedException {
    if (_process == null)
      throw new IllegalStateException("you must call start first");

    _out.join();
    _err.join();
    return _process.waitFor();
  }

  /**
   * Properly waits until everything is complete: joins on the thread that
   * reads the output, joins on the thread that reads the error and finally
   * wait for the process to be finished.
   * If the process has not completed before the timeout, throws a {@link TimeoutException}
   * @return the status code of the process.
   * @throws TimeoutException the process timed out
   * @throws InterruptedException the process was interrupted before completion
   */
  public int waitFor(long timeout) throws InterruptedException, TimeoutException {
    if (_process == null)
      throw new IllegalStateException("you must call start first");

    // Chronos c = new Chronos();
    _out.join(timeout);
    // timeout -= c.tick();
    if (timeout <= 0)
      throw new TimeoutException("Wait timed out");
    _err.join(timeout);
    // timeout -= c.tick();
    if (timeout <= 0)
      throw new TimeoutException("Wait timed out");

    // there is no timeout in this API, not much we can do here
    // waiting on the other two threads should give us some safety
    return _process.waitFor();
  }

  /**
   * @see {@link Process#exitValue()}
   * @return the return code of the process
   */
  public int exitValue() {
    if (_process == null)
      throw new IllegalStateException("you must call start first");

    return _process.exitValue();
  }

  /**
   * see {@link Process#destroy()}
   */
  public void destroy() {
    if (_process == null)
      throw new IllegalStateException("you must call start first");

    _process.destroy();
  }

  /**
   * Creates an external process from the command. It is not started and you have to call
   * start on it!
   * @param commands the command to execute
   * @return the process
   */
  public static ExternalCommand create(String... commands) {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    return ec;
  }

  /**
   * Creates an external process from the command. It is not started and you have to call
   * start on it!
   * @param commands the command to execute
   * @return the process
   */
  public static ExternalCommand create(List<String> commands) {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    return ec;
  }

  /**
   * Creates an external process from the command. The command is executed.
   * @param commands the commands to execute
   * @return the process
   * @throws IOException if there is an error
   */
  public static ExternalCommand start(String... commands) throws IOException {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    ec.start();
    return ec;
  }

  /**
   * Executes the external command in the given working directory and waits for it to be
   * finished.
   * @param workingDirectory the root directory from where to run the command
   * @param command the command to execute (should be relative to the working directory
   * @param args the arguments to the command
   * @return the process
   */
  public static ExternalCommand execute(File workingDirectory, String command, String... args)
      throws IOException, InterruptedException {
    try {
      return executeWithTimeout(workingDirectory, command, 0, args);
    } catch (TimeoutException e) {
      // Can't happen!
      throw new IllegalStateException(MODULE + ".execute: Unexpected timeout occurred!");
    }
  }

  /**
   * Executes the external command in the given working directory and waits (until timeout
   * is elapsed) for it to be finished.
   * @param workingDirectory
   *          the root directory from where to run the command
   * @param command
   *          the command to execute (should be relative to the working directory
   * @param timeout
   *          the maximum amount of time to wait for this external command (in ms). If
   *          this value is less than or equal to 0, timeout is ignored
   * @param args
   *          the arguments to the command
   * @return the process
   */
  public static ExternalCommand executeWithTimeout(File workingDirectory, String command,
      long timeout, String... args) throws IOException, InterruptedException, TimeoutException {
    List<String> arguments = new ArrayList<String>(args.length + 1);

    arguments.add(new File(workingDirectory, command).getAbsolutePath());
    arguments.addAll(Arrays.asList(args));

    ExternalCommand cmd = ExternalCommand.create(arguments);

    cmd.setWorkingDirectory(workingDirectory);

    cmd.setRedirectErrorStream(true);

    cmd.start();

    /* Use timeout if it is a valid value! */
    if (timeout <= 0)
      cmd.waitFor();
    else
      cmd.waitFor(timeout);

    if (LOG.isDebugEnabled())
      LOG.debug(cmd.getStringOutput());

    return cmd;
  }
}
