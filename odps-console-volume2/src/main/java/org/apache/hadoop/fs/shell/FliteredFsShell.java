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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.find.Find;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class FliteredFsShell extends FsShell
{
    public FliteredFsShell(Configuration conf) {
        super(conf);
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

        factory.addClass(Ls.class, "-ls");
        factory.addClass(Ls.Lsr.class, "-lsr");

        factory.addClass(Mkdir.class, "-mkdir");

        factory.addClass(MoveCommands.MoveFromLocal.class, "-moveFromLocal");
        factory.addClass(MoveCommands.MoveToLocal.class, "-moveToLocal");
        factory.addClass(MoveCommands.Rename.class, "-mv");

        factory.addClass(SetReplication.class, "-setrep");

        factory.addClass(Tail.class, "-tail");

        factory.addClass(Test.class, "-test");

        factory.addClass(Touch.Touchz.class, "-touchz");
    }
}
