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

package com.aliyun.openservices.odps.console.volume2;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.FliteredFsShell;

import java.io.PrintStream;

public class Volume2Command extends AbstractCommand
{
    private static final String COMMAND_IDENTITY = "vfs";
    public static final String[] HELP_TAGS = new String[]{COMMAND_IDENTITY};
    private static FliteredFsShell shell = null;
    private String[] args = null;

    public Volume2Command(String commandText, ExecutionContext context) {
        super(commandText, context);
        String[] commandSplits = commandText.split("\\s+");
        args = new String[commandSplits.length - 1];
        System.arraycopy(commandSplits, 1, args, 0, commandSplits.length - 1);
    }

    @Override
    public void run() throws OdpsException, ODPSConsoleException {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            shell.run(args);
            shell.close();
            shell = null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public static void printUsage(PrintStream out) {
        try {
            new FliteredFsShell(new Configuration()).run(new String[]{"-usage"});
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Volume2Command parse(String command, ExecutionContext sessionContext) {
        String trimCmd = command.trim().replaceAll("\\s+", " ");
        if (trimCmd.startsWith(COMMAND_IDENTITY) || COMMAND_IDENTITY.equals(trimCmd)) {
            if (shell == null) {
                shell = new FliteredFsShell(
                    new Configuration() {{
                        set("odps.access.id", sessionContext.getAccessId());
                        set("odps.access.key", sessionContext.getAccessKey());
                        set("odps.service.endpoint", sessionContext.getEndpoint());
                        set("fs.defaultFS", String.format("odps://%s/", sessionContext.getProjectName()));
                        set("fs.odps.impl", "com.aliyun.odps.fs.VolumeFileSystem");
                        set("dfs.replication", "3");
                        set("fs.AbstractFileSystem.odps.impl", "com.aliyun.odps.fs.VolumeFs");
                        set("io.file.buffer.size", "512000");
                        set("volume.internal", "false");
                        set("pangu.access.check", "false");
                        setClassLoader(this.getClass().getClassLoader());
                    }}
                );
            }
            return new Volume2Command(command, sessionContext);
        }
        return null;
    }
}
