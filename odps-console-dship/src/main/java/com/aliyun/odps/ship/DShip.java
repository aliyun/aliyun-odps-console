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

package com.aliyun.odps.ship;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ship.common.CommandType;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.download.DshipDownload;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.ship.upload.TunnelUpdateSession;
import com.aliyun.odps.ship.upload.TunnelUploadSession;
import com.aliyun.odps.ship.upsert.DshipUpdate;
import com.aliyun.odps.ship.upsert.TunnelUpsertSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class DShip {

  // leaving this function alone is for odpscmd integration
  public static CommandType parseSubCommand(String[] args) {
    CommandType type;
    try {
      type = CommandType.fromString(args[0]);
    } catch (IllegalArgumentException e) {
      System.err.println("Unknown command: '" + args[0] + "'.");
      System.err.println("Type 'tunnel help' for usage.");
      throw(e);
    }
    return type;
  }

  public static void runSubCommand(CommandType type, String[] args)
      throws Throwable {
    String sid = null;
    try {
      switch (type) {
        case upload:
          OptionsBuilder.buildUploadOption(args);
          DshipUpdate uploader = new DshipUpdate(new TunnelUploadSession());
          sid = uploader.getTunnelSessionId();
          uploader.upload();
          break;
        case download:
          OptionsBuilder.buildDownloadOption(args);
          DshipDownload downloader = new DshipDownload();
          downloader.download();
          break;
        case upsert:
          OptionsBuilder.buildUpsertOption(args);
          DshipUpdate upserter = new DshipUpdate(new TunnelUpsertSession());
          sid = upserter.getTunnelSessionId();
          upserter.upload();
          break;
        case resume:
          resume(args);
          break;
        case show:
          show(args);
          break;
        case purge:
          purge(args);
          break;
        case help:
          help(args);
          break;
        default:
          throw new IllegalArgumentException(Constants.ERROR_INDICATOR + "Unknown command");
      }
    } catch(UserInterruptException e) {
      throw(e);
    } catch (IllegalArgumentException e) {
      logException(sid, e);
      throw(e);
    } catch (FileNotFoundException e) {
      logException(sid, e);
      throw(e);
    } catch (TunnelException e) {
      logExceptionWithCause(sid, Constants.ERROR_INDICATOR + "TunnelException - ", e);
      throw(e);
    } catch (OdpsException e) {
      logExceptionWithCause(sid, Constants.ERROR_INDICATOR + "OdpsException - ", e);
      throw(e);
    } catch (IOException e) {
      logExceptionWithCause(sid, Constants.ERROR_INDICATOR + "IOException - ", e);
      throw(e);
    } catch (ParseException e) {
      logException(sid, e);
      throw(e);
    } catch (Throwable e) {
      logException(
          sid, new Exception(Constants.ERROR_INDICATOR + "Unknown error - " + e.getMessage(), e));
      throw(e);
    }
  }

  public static void resume(String[] args)
      throws OdpsException, IOException, ParseException, InvalidParameterException,
             ODPSConsoleException {
    System.out.println("start resume");
    OptionsBuilder.buildResumeOption(args);
    String sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    System.out.println(sid);
    SessionHistory sh = null;
    if (sid == null) {
      sh = SessionHistoryManager.getLatest();
      sid = sh.getSid();
      System.out.println(sid);
    } else {
      Util.checkSession(sid);
      sh = SessionHistoryManager.createSessionHistory(sid);
    }

    sh.loadContext();
    String type = DshipContext.INSTANCE.get(Constants.COMMAND_TYPE);
    if (type == null || !(type.equals("upload") || type.equals("upsert"))) {
      throw new InvalidParameterException(
          Constants.ERROR_INDICATOR + "not support resume for '" + type + "'");
    }

    sh.log("start resume");
    TunnelUpdateSession uploadSession;
    switch (type) {
      case "upsert":
        uploadSession = new TunnelUpsertSession();
        break;
      case "upload":
      default:
        uploadSession = new TunnelUploadSession();
    }
    DshipUpdate uploader = new DshipUpdate(uploadSession);
    uploader.upload();
    sh.log("resume complete");
  }

  private static void show(String[] args) throws ParseException, IOException {

    OptionsBuilder.buildShowOption(args);
    String cmd = DshipContext.INSTANCE.get(Constants.SHOW_COMMAND);
    String sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    if ("history".equals(cmd)) {
      int n = DshipContext.INSTANCE.get("number") == null ? 20 : Integer.valueOf(
          DshipContext.INSTANCE.get("number"));
      SessionHistoryManager.showHistory(n);
    } else if ("log".equals(cmd)) {
      Util.checkSession(sid);
      SessionHistory sh = sid == null ? SessionHistoryManager.getLatestSilently() :
          SessionHistoryManager.createSessionHistory(sid);
      if (sh != null) {
        sh.showLog();
      } else {
        logWarning("previous session not found.");
      }
    } else if ("bad".equals(cmd)) {
      Util.checkSession(sid);
      SessionHistory sh = sid == null ? SessionHistoryManager.getLatestSilently() :
          SessionHistoryManager.createSessionHistory(sid);
      if (sh != null) {
        sh.showBad();
      } else {
        logWarning("previous session not found.");
      }
    } else {
      throw new ParseException("Unknown command: '" + cmd + "'\nType 'tunnel help show' for usage.");
    }
  }

  private static void purge(String[] args) throws ParseException, IOException {
    OptionsBuilder.buildPurgeOption(args);
    int n = Integer.valueOf(DshipContext.INSTANCE.get(Constants.PURGE_NUMBER));
    SessionHistoryManager.purgeHistory(n);
  }

  private static void help(String[] args) throws ParseException, IOException {

    OptionsBuilder.buildHelpOption(args);
    String cmd = DshipContext.INSTANCE.get(Constants.HELP_SUBCOMMAND);
    if (DshipContext.INSTANCE.get(Constants.HELP_SUBCOMMAND) == null) {
      cmd = "help";
    }

    HelpFormatter formatter = new HelpFormatter();
    formatter.setLongOptPrefix("-");
    CommandType type = CommandType.fromString(cmd);
    switch (type) {
      case upload:
        formatter.printHelp("tunnel upload [options] <path> <[project.]table[/partition]>\n"
                            + "\tupload data from local file",
                            OptionsBuilder.getUploadOptions());
        showHelp("upload.txt");
        break;
      case download:
        formatter.printHelp("tunnel download [options] <[project.]table[/partition]> <path>\n"
                            + "\tdownload data to local file",
                            OptionsBuilder.getDownloadOptions());
        formatter.printHelp("tunnel download [options] instance://<[project/]instance_id> <path>\n"
                            + "\tdownload instance result to local file",
                            OptionsBuilder.getDownloadOptions());
        showHelp("download.txt");
        break;
      case upsert:
        formatter.printHelp("tunnel upsert [options] <path> <[project.]table[/partition]>\n"
                            + "\tupsert data from local file",
                            OptionsBuilder.getUpsertOptions());
        showHelp("upsert.txt");
        break;
      case resume:
        formatter.printHelp("tunnel resume [session_id] [-force]\n"
                            + "\tresume an upload session",
                            OptionsBuilder.getResumeOptions());
        showHelp("resume.txt");
        break;
      case show:
        formatter.printHelp("tunnel show history [options]\n"
                            + "\tshow session information",
                            OptionsBuilder.getShowOptions());
        showHelp("show.txt");
        break;
      case purge:
        formatter.printHelp("tunnel purge [n]\n"
                            + "\tforce session history to be purged.([n] days before, default "
                            + Constants.DEFAULT_PURGE_NUMBER + " days)",
                            OptionsBuilder.getPurgeOptions());
        showHelp("purge.txt");
        break;
      case help:
        showHelp("help.txt");
        break;
      default:
        break;
    }
  }

  private static void logException(String sid, Exception e) {
    logExceptionWithCause(sid, "", e);
  }

  private static void logWarning(String message) {
    System.err.println(message);
  }

  private static void logExceptionWithCause(String sid, String cause, Exception e) {
    logWarning(cause + e.getMessage());
    if (sid == null) {
      sid = ".sidnull";
    }

    try {
      SessionHistory sh = SessionHistoryManager.createSessionHistory(sid);
      sh.log(Util.getStack(e));
    } catch (Exception e1) {
      // do nothing
    }
  }

  protected static void showHelp(String filename) throws IOException {

    InputStream ins = DShip.class.getResourceAsStream("/" + filename);
    try {
      InputStreamReader reader = new InputStreamReader(ins, "utf-8");
      int c = reader.read();
      while (c != -1) {
        System.out.print((char) c);
        c = reader.read();
      }
    } finally {
      IOUtils.closeQuietly(ins);
    }
  }

}
