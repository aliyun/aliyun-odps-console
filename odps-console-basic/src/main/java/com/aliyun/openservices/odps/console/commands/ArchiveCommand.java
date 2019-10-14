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

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import com.google.gson.GsonBuilder;
import org.jline.reader.UserInterruptException;

/**
 * Created by yinyue.yy on 2014/10/14.
 */
public class ArchiveCommand extends MultiClusterCommandBase {

    public static final String[] HELP_TAGS = new String[]{"archive"};

    public static void printUsage(PrintStream stream) {
        stream.println("Usage: alter table <tablename> archive");
    }

    private String taskName = "";
    private final String ARCHIVE_FLAG = "odps.merge.archive.flag";
    private final String ARCHIVE_SETTING = "archiveSettings";

    public void run() throws OdpsException, ODPSConsoleException,
            ODPSConsoleException {

        ExecutionContext context = getContext();
        DefaultOutputWriter writer = context.getOutputWriter();

        // do retry
        int retryTime = context.getRetryTimes();
        retryTime = retryTime > 0 ? retryTime : 1;
        while (true) {
            try {
                MergeTask task = null;
                taskName = "console_archive_task_"
                        + Calendar.getInstance().getTimeInMillis();
                task = new MergeTask(taskName, getCommandText());

                for (Entry<String, String> property : QueryUtil.getTaskConfig().entrySet()) {
                    task.setProperty(property.getKey(), property.getValue());
                }

                Map<String, String> archiveSettings = new HashMap<String, String>();
                archiveSettings.put(ARCHIVE_FLAG, "true");
                Task.Property property = new Task.Property(ARCHIVE_SETTING,
                        new GsonBuilder().disableHtmlEscaping().create().toJson(archiveSettings));

                task.setProperty(property.getName(), property.getValue());

                runJob(task);
                // success
                writer.writeError("OK");

                break;
            } catch (UserInterruptException e) {
                throw e;
            } catch (Exception e) {
                if (--retryTime <= 0) {
                    throw new ODPSConsoleException(e.getMessage());
                }
                writer.writeError("retry " + retryTime);
                writer.writeDebug(StringUtils.stringifyException(e));
            }
        }
    }

    public String getTaskName() {
        return taskName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public ArchiveCommand(String commandText, ExecutionContext context) {
        super(commandText, context);
    }

    public static ArchiveCommand parse(String commandString,
                                     ExecutionContext sessionContext) {
        String content = commandString;
        String regstr = "\\s*ALTER\\s+TABLE\\s+(.*)(ARCHIVE\\s*)$";

        Pattern p = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(content);

        if (m.find()) {
            // extract the table/partition info
            return new ArchiveCommand(m.group(1), sessionContext);
        }
        return null;
    }
}

