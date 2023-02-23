package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;

import org.apache.hadoop.fs.FileStatus;

public class VfsLsCommand extends Ls {
    public static final String NAME = "ls";
    public static final String USAGE = "[-d] [-h] [-R] [<path> ...]";
    public static final String DESCRIPTION = "List the contents that match the specified file pattern. If path is not specified, the contents of /user/<currentUser> will be listed. Directory entries are of the form:\n\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\nand file entries are of the form:\n\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n-d:  Directories are listed as plain files.\n-h:  Formats the sizes of files in a human-readable fashion rather than a number of bytes.\n-R:  Recursively list the contents of directories.";

    public VfsLsCommand() {
        super();
    }

    protected void processPath(PathData item) throws IOException {
        FileStatus stat = item.stat;
        String line = String.format(this.lineFormat, stat.isDirectory() ? "d" : "-", stat.getPermission() + (stat.getPermission().getAclBit() ? "+" : " "), stat.isFile() ? stat.getReplication() : "-", stat.getOwner(), stat.getGroup(), this.formatSize(stat.getLen()), this.dateFormat.format(new Date(stat.getModificationTime())), item);

        if (stat.isSymlink()) {
            line += (" -> " + stat.getSymlink());
        }

        this.out.println(line);
    }

    public static class VfsLsr extends VfsLsCommand {
        public static final String NAME = "lsr";

        public VfsLsr() {
        }

        protected void processOptions(LinkedList<String> args) throws IOException {
            args.addFirst("-R");
            super.processOptions(args);
        }

        public String getReplacementCommand() {
            return "ls -R";
        }
    }
}
