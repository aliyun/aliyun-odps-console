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

package org.apache.hadoop.fs.shell;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.find.Find;
import org.apache.hadoop.tools.TableListing;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.volume2.CreateExternalVolumeCommand;

public class FilteredFsShell extends FsShell
{
    private static final int MAX_LINE_WIDTH = 80;
    private static final String usagePrefix = "Usage: vfs ";

    public FilteredFsShell(Configuration conf) {
        super(conf);
    }

    protected void init() {
        getConf().setQuietMode(true);
        if (commandFactory == null) {
            commandFactory = new CommandFactory(getConf());
            commandFactory.addObject(new Help(), "-help");
            commandFactory.addObject(new Usage(), "-usage");
            registerCommands(commandFactory);
        }
    }

    protected class Usage extends FsCommand {
        public static final String NAME = "usage";
        public static final String USAGE = "[cmd ...]";
        public static final String DESCRIPTION =
                "Displays the usage for given command or all commands if none " +
                        "is specified.";

        @Override
        protected void processRawArguments(LinkedList<String> args) {
            if (args.isEmpty()) {
                printUsage(System.out, null);
            } else {
                for (String arg : args) printUsage(System.out, arg);
            }
        }
    }

    // print one usage
    public void printUsage(PrintStream out, String cmd) {
        printInfo(out, cmd, false);
    }

    private void printHelp(PrintStream out) {
        this.printInfo(out, (String)null, true);
    }

    protected class Help extends FsCommand {
        public static final String NAME = "help";
        public static final String USAGE = "[cmd ...]";
        public static final String DESCRIPTION =
                "Displays help for given command or all commands if none " +
                        "is specified.";

        @Override
        protected void processRawArguments(LinkedList<String> args) {
            if (args.isEmpty()) {
                printHelp(System.out, null);
            } else {
                for (String arg : args) printHelp(System.out, arg);
            }
        }
    }


    public void printInfo(PrintStream out, String cmd, boolean showHelp) {
        if (cmd != null) {
            // display help or usage for one command
            Command instance = commandFactory.getInstance("-" + cmd);
            if (instance == null) {
                throw new IllegalArgumentException(cmd);
            }
            if (showHelp) {
                printInstanceHelp(out, instance);
            } else {
                out.println(usagePrefix + instance.getUsage());
            }
        } else {
            // display help or usage for all commands
            out.println(usagePrefix);

            // display list of short usages
            ArrayList<Command> instances = new ArrayList<Command>();
            for (String name : commandFactory.getNames()) {
                Command instance = commandFactory.getInstance(name);
                if (!instance.isDeprecated()) {
                    out.println("\t[" + instance.getUsage() + "]");
                    instances.add(instance);
                }
            }
            // display long descriptions for each command
            if (showHelp) {
                for (Command instance : instances) {
                    out.println();
                    printInstanceHelp(out, instance);
                }
            }
            out.println();
        }
    }

    private void displayError(String cmd, String message) {
        for (String line : message.split("\n")) {
            System.err.println(cmd + ": " + line);
            if (cmd.charAt(0) != '-') {
                Command instance = null;
                instance = commandFactory.getInstance("-" + cmd);
                if (instance != null) {
                    System.err.println("Did you mean -" + cmd + "?  This command " +
                            "begins with a dash.");
                }
            }
        }
    }

    @Override
    public int run(String argv[]) {
        // initialize FsShell
        init();

        int exitCode = -1;
        if (argv.length < 1) {
            printUsage(System.err);
        } else {
            String cmd = argv[0];
            Command instance = null;
            try {
                instance = commandFactory.getInstance(cmd);
                if (instance == null) {
                    throw new ODPSConsoleException("Unknown command: " + cmd);
                }
                if (instance instanceof VfsLsCommand) {
                    getConf().set("odps.restclient.retrytime", "0");
                }
                exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
            } catch (IllegalArgumentException e) {
                displayError(cmd, e.getLocalizedMessage());
                if (instance != null) {
                    printInstanceUsage(System.err, instance);
                }
            } catch (Exception e) {
                // instance.run catches IOE, so something is REALLY wrong if here
                displayError(cmd, "Fatal internal error");
                e.printStackTrace(System.err);
            }
        }
        return exitCode;
    }

    private void printUsage(PrintStream out) {
        this.printInfo(out, (String)null, false);
    }

    public void printInstanceUsage(PrintStream out, Command instance) {
        out.println(usagePrefix + instance.getUsage());
    }

    private void printInstanceHelp(PrintStream out, Command instance) {
        out.println(instance.getUsage() + " :");
        TableListing listing = null;
        final String prefix = "  ";
        for (String line : instance.getDescription().split("\n")) {
            if (line.matches("^[ \t]*[-<].*$")) {
                String[] segments = line.split(":");
                if (segments.length == 2) {
                    if (listing == null) {
                        listing = new TableListing.Builder().addField("").addField("", true)
                                .wrapWidth(MAX_LINE_WIDTH).build();
                    }
                    listing.addRow(segments[0].trim(), segments[1].trim());
                    continue;
                }
            }

            // Normal literal description.
            if (listing != null) {
                for (String listingLine : listing.toString().split("\n")) {
                    out.println(prefix + listingLine);
                }
                listing = null;
            }

            for (String descLine : WordUtils.wrap(
                    line, MAX_LINE_WIDTH, "\n", true).split("\n")) {
                out.println(prefix + descLine);
            }
        }

        if (listing != null) {
            for (String listingLine : listing.toString().split("\n")) {
                out.println(prefix + listingLine);
            }
        }
    }

    // print one help
    private void printHelp(PrintStream out, String cmd) {
        printInfo(out, cmd, true);
    }

    @Override
    protected void registerCommands(CommandFactory factory)
    {
        // VolumeFileSystem doesn't support
        /*
        factory.registerCommands(AclCommands.class);
            factory.addClass(AclCommands.GetfaclCommand.class, "-" + GET_FACL);
            factory.addClass(AclCommands.SetfaclCommand.class, "-" + SET_FACL);

        factory.registerCommands(CopyCommands.class);
            factory.addClass(AppendToFile.class, "-appendToFile");

        factory.registerCommands(Count.class);

        factory.registerCommands(Delete.class);
            factory.addClass(Delete.Expunge.class, "-expunge");

        factory.registerCommands(Display.class);
            factory.addClass(Display.Checksum.class, "-checksum");

        factory.registerCommands(Find.class);

        factory.registerCommands(FsShellPermissions.class);
            factory.addClass(Chmod.class, "-chmod");
            factory.addClass(Chown.class, "-chown");
            factory.addClass(Chgrp.class, "-chgrp");

        factory.registerCommands(FsUsage.class); // du 性能太差了, 得重新实现一版
            factory.addClass(Df.class, "-df");
            factory.addClass(Du.class, "-du");
            factory.addClass(Dus.class, "-dus");

        factory.registerCommands(Ls.class);

        factory.registerCommands(Mkdir.class);

        factory.registerCommands(MoveCommands.class);

        factory.registerCommands(SetReplication.class);

        factory.registerCommands(Stat.class); // 这个目前是可以执行的，但是数据返回没有意义
            factory.registerCommands(SetReplication.class);

        factory.registerCommands(Tail.class);

        factory.registerCommands(Test.class);

        factory.registerCommands(Touch.class);

        factory.registerCommands(Truncate.class);
            factory.addClass(Truncate.class, "-truncate");

        factory.registerCommands(SnapshotCommands.class);
            factory.addClass(CreateSnapshot.class, "-" + CREATE_SNAPSHOT);
            factory.addClass(DeleteSnapshot.class, "-" + DELETE_SNAPSHOT);
            factory.addClass(RenameSnapshot.class, "-" + RENAME_SNAPSHOT);

        factory.registerCommands(XAttrCommands.class);
            factory.addClass(GetfattrCommand.class, "-" + GET_FATTR);
            factory.addClass(SetfattrCommand.class, "-" + SET_FATTR);
         */

        factory.addClass(CopyCommands.Merge.class, "-getmerge");
        factory.addClass(CopyCommands.Cp.class, "-cp");
        factory.addClass(CopyCommands.CopyFromLocal.class, "-copyFromLocal");
        factory.addClass(CopyCommands.CopyToLocal.class, "-copyToLocal");
        factory.addClass(CopyCommands.Get.class, "-get");
        factory.addClass(CopyCommands.Put.class, "-put");

        factory.addClass(Count.class, "-count");

        factory.addClass(Delete.Rm.class, "-rm");
        factory.addClass(Delete.Rmdir.class, "-rmdir");
        factory.addClass(Delete.Rmr.class, "-rmr");

        factory.addClass(Display.Cat.class, "-cat");
        factory.addClass(Display.Text.class, "-text");

        factory.addClass(Find.class, "-find");

        factory.addClass(VfsLsCommand.class, "-ls");
        factory.addClass(VfsLsCommand.VfsLsr.class, "-lsr");

        factory.addClass(Mkdir.class, "-mkdir");

        factory.addClass(MoveCommands.MoveFromLocal.class, "-moveFromLocal");
        factory.addClass(MoveCommands.MoveToLocal.class, "-moveToLocal");
        factory.addClass(MoveCommands.Rename.class, "-mv");

        factory.addClass(SetReplication.class, "-setrep");

        factory.addClass(Tail.class, "-tail");

        factory.addClass(Test.class, "-test");

        factory.addClass(TouchCommands.Touch.Touchz.class, "-touchz");
    }
}
