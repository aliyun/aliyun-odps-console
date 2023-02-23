package com.aliyun.openservices.odps.console.volume2;

import java.io.PrintStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Volume;
import com.aliyun.odps.Volumes;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class CreateExternalVolumeCommand extends ExternalVolumeCommand {

    private static final String OP_SP = "storage_provider";
    private static final String OP_URL = "url";
    private static final String OP_RA = "role_arn";
    private static final String OP_LIFECYCLE = "lifecycle";

    private static List<String> SUB_COMMAND_WHITELIST = null;

    private String[] args = null;

    private String storageProvider = null;
    private String externalLocation = null;
    private String roleArn = null;
    private Long lifecycle = null;
    private String comment = null;
    private String extVolumeName = null;


    public static void printCreateExtVolumeUsage(PrintStream out) {
        out.println("");
        out.println("Usage: vfs <-create > <volume_name> <-storage_provider> <type> <-url> <external_url> [-role_arn <role_arn>] [-lifecycle <number>] [comment];");
    }

    public CreateExternalVolumeCommand(String commandText, ExecutionContext context) {
        super(commandText, context);
    }

    @Override
    public void run() throws OdpsException, ODPSConsoleException {
        String[] commandSplits = getCommandText().split("\\s+");

        if (commandSplits.length < 7) {
            printCreateExtVolumeUsage(System.err);
            throw new ODPSConsoleException("Invalid create external volume command.");
        }

        extVolumeName = commandSplits[2].toLowerCase();
        args = new String[commandSplits.length - 3];
        System.arraycopy(commandSplits, 3, args, 0, commandSplits.length - 3);

        createExternalVolume();

        getWriter().writeError("OK");
    }


    private void createExternalVolume() throws OdpsException, ODPSConsoleException {
        processCreateOptions(args);

        if (storageProvider.equalsIgnoreCase("oss")) {
            Volumes.VolumeBuilder builder = new Volumes.VolumeBuilder();

            builder.project(getCurrentProject()).volumeName(extVolumeName).type(Volume.Type.EXTERNAL).extLocation(externalLocation);

            if (lifecycle != null) {
                builder.lifecycle(lifecycle);
            }

            if (comment != null) {
                builder.comment(comment);
            }

            if (roleArn != null) {
                builder.addProperty(Volumes.EXTERNAL_VOLUME_ROLEARN_KEY, roleArn);
            }

            getCurrentOdps().volumes().create(builder);

        } else {
            throw new ODPSConsoleException("Illegal storage provider: " + storageProvider);
        }
    }

    private Options initOptions() {
        Options opts = new Options();
        Option sp = new Option(OP_SP, OP_SP, true, "storage provider");
        sp.setRequired(true);

        Option url= new Option(OP_URL, OP_URL, true, "external location url");
        url.setRequired(true);

        Option role = new Option(OP_RA, OP_RA, true, "role arn");
        role.setRequired(false);

        Option lifecycle = new Option(OP_LIFECYCLE, OP_LIFECYCLE, true, "lifecycle");
        lifecycle.setRequired(false);

        opts.addOption(sp);
        opts.addOption(url);
        opts.addOption(role);
        opts.addOption(lifecycle);

        return opts;
    }

    protected void processCreateOptions(String[] args) throws ODPSConsoleException {
        Options opts = initOptions();
        CommandLineParser clp = new DefaultParser();
        CommandLine cl;
        try {
            cl = clp.parse(opts, args, false);
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal command: " + e.getMessage(), e);
        }

        if (cl.hasOption(OP_SP)) {
            storageProvider = cl.getOptionValue(OP_SP);
        } else {
            printCreateExtVolumeUsage(System.err);
            throw new ODPSConsoleException(OP_SP + "parameter is required.");
        }

        if (cl.hasOption(OP_URL)) {
            // cannot to lowercase
            externalLocation = cl.getOptionValue(OP_URL);
        } else {
            printCreateExtVolumeUsage(System.err);
            throw new ODPSConsoleException(OP_URL + "parameter is required.");
        }

        if (cl.hasOption(OP_RA)) {
            // cannot to lowercase
            roleArn = cl.getOptionValue(OP_RA);
        }

        if (cl.hasOption(OP_LIFECYCLE)) {
            lifecycle = Long.parseLong(cl.getOptionValue(OP_LIFECYCLE));
        }

        List<String> leftArgs = cl.getArgList();
        if (leftArgs.size() > 1) {
            printCreateExtVolumeUsage(System.err);
            throw new ODPSConsoleException("Invalid parameters: " + String.join(", ", leftArgs));
        } else if (leftArgs.size() == 1) {
            comment = leftArgs.get(0);
        }
    }
}
