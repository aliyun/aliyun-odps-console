package com.aliyun.openservices.odps.console.volume2;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import org.junit.Test;

public class Volume2CommandTest
{
    @Test
    public void testVolume2() throws ODPSConsoleException, OdpsException
    {
        Volume2Command.printUsage(null);
        ExecutionContext ctx = ExecutionContext.init();
        Volume2Command command =
            Volume2Command
                .parse(
                    "vfs -ls /",
                    ctx);
        assert command != null;
        command.run();
    }
}
