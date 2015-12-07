/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console;

/**
 * @author shuman.gansm
 * */
public class ODPSConsoleException extends Exception {

  private static final long serialVersionUID = -2966834001339971270L;

  // default 1
  private int exitCode = 1;

  public ODPSConsoleException() {
    super();
  }

  public ODPSConsoleException(String message, Throwable cause) {
    super(message, cause);
  }

  public ODPSConsoleException(String message, Throwable cause, int exitCode) {
    super(message, cause);
    this.exitCode = exitCode;
  }

  public ODPSConsoleException(String message) {
    super(message);
  }

  public ODPSConsoleException(Throwable cause) {
    super(cause);
  }

  public int getExitCode() {
    return exitCode;
  }

}
