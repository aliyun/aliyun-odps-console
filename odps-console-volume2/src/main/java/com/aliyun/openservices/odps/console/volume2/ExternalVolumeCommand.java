package com.aliyun.openservices.odps.console.volume2;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;

import com.aliyun.odps.OdpsException;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.PluginUtil;

public class ExternalVolumeCommand extends AbstractCommand {

    public static final String[] HELP_TAGS = new String[]{"vfs", "volume"};

    public static final String CREATE_SUB_COMMAND = "-create";
    public static final String DELETE_SUB_COMMAND = "-rmv";

    private static List<String> SUB_COMMAND_WHITELIST = null;

    static {
        try {
            Properties properties = PluginUtil.getPluginProperty(CreateExternalVolumeCommand.class);
            String cmd = properties.getProperty("external_volume_sub_commands");
            if (!StringUtils.isNullOrEmpty(cmd)) {
                SUB_COMMAND_WHITELIST = Arrays.asList(cmd.split(","));
            }

        } catch (IOException e) {
            // a warning
            System.err.println("Warning: load config failed, cannot get external volume sub commands fields.");
            System.err.flush();
        }

    }

    public static void printUsage(PrintStream out, ExecutionContext ctx) {
        try {
            CreateExternalVolumeCommand.printCreateExtVolumeUsage(out);
            DeleteExternalVolumeCommand.printDeleteExternalVolumeUsage(out);

            if (CollectionUtils.isNotEmpty(SUB_COMMAND_WHITELIST)) {
                SUB_COMMAND_WHITELIST.forEach(cmd->Volume2Command.printCommandUsage(out, cmd, ctx));
            } else {
                Volume2Command.printUsage(out);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ExternalVolumeCommand(String commandText, ExecutionContext context) {
        super(commandText, context);
    }

    public static ExternalVolumeCommand parse(String commandString, ExecutionContext sessionContext) throws ODPSConsoleException {
        String trimCmd = commandString.trim().replaceAll("\\s+", " ");
        if (trimCmd.startsWith(Volume2Command.COMMAND_IDENTITY) || Volume2Command.COMMAND_IDENTITY.equals(trimCmd)) {
            String[] commandSplits = trimCmd.split("\\s+");

            if (commandSplits.length >= 3) {
                String subCommand = commandSplits[1];
                if (subCommand.equalsIgnoreCase(CREATE_SUB_COMMAND)) {
                    return new CreateExternalVolumeCommand(commandString, sessionContext);
                }

                if (subCommand.equalsIgnoreCase(DELETE_SUB_COMMAND)) {
                    return new DeleteExternalVolumeCommand(commandString, sessionContext);
                }

                if (CollectionUtils.isNotEmpty(SUB_COMMAND_WHITELIST) && !SUB_COMMAND_WHITELIST.contains(subCommand)) {
                    throw new ODPSConsoleException("Invalid sub command: " + subCommand);
                }

                return null;
            }

            return new ExternalVolumeCommand(commandString, sessionContext);
        }

        return null;
    }


    @Override
    public void run() throws OdpsException, ODPSConsoleException {
        String[] commandSplits = getCommandText().split("\\s+");

        if (commandSplits.length == 1 || ((commandSplits.length == 2) && commandSplits[1].equalsIgnoreCase("-usage"))) {
            ExternalVolumeCommand.printUsage(System.err, getContext());
        } else {
            ExternalVolumeCommand.printUsage(System.err, getContext());
            throw new ODPSConsoleException("Invalid command: " + getCommandText());
        }
    }
}
