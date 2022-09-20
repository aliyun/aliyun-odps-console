package com.aliyun.openservices.odps.console.commands;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class AlterTableCommandTest {

  private static String[] positivesPair = {
      "alter table name   archive",
      "name",
      "alTEr Table name aRchive",
      "name",
      "alter table name partition (dt ='2020-1-1',st='2020-1-1') archive",
      "name partition (dt ='2020-1-1',st='2020-1-1')"
  };

  private static String[] negativeCmds = {
      "alter table name Archives",
      "alter table name\npartition(dt='xxx') Archive",
      "alter table name xarchive",
      "alter table name partition (dt='xxx')"
  };

  private static String negativeLongCmd =
      "alter table name "
      + "partition(dt = '2021-10-12', st = '2021-10-12') "
      + "partition(dt = '2021-10-12', st = '2021-10-11') "
      + "partition(dt = '2021-10-12', st = '2021-10-10') "
      + "partition(dt = '2021-10-12', st = '2021-10-09') "
      + "partition(dt = '2021-10-12', st = '2021-10-08') "
      + "partition(dt = '2021-10-12', st = '2021-10-07') "
      + "partition(dt = '2021-10-12', st = '2021-10-06') "
      + "partition(dt = '2021-10-12', st = '2021-10-05') "
      + "partition(dt = '2021-10-12', st = '2021-10-04') "
      + "partition(dt = '2021-10-12', st = '2021-10-03') "
      + "partition(dt = '2021-10-12', st = '2021-10-02') "
      + "partition(dt = '2021-10-12', st = '2021-10-01') "
      + "partition(dt = '2021-10-12', st = '2021-09-30') "
      + "partition(dt = '2021-10-12', st = '2021-09-29') "
      + "partition(dt = '2021-10-12', st = '2021-09-28') "
      + "partition(dt = '2021-10-12', st = '2021-09-27') "
      + "partition(dt = '2021-10-12', st = '2021-09-26') "
      + "partition(dt = '2021-10-12', st = '2021-09-25') "
      + "partition(dt = '2021-10-12', st = '2021-09-24') "
      + "partition(dt = '2021-10-12', st = '2021-09-23') "
      + "partition(dt = '2021-10-12', st = '2021-09-22') "
      + "partition(dt = '2021-10-12', st = '2021-09-21') "
      + "partition(dt = '2021-10-12', st = '2021-09-20') "
      + "partition(dt = '2021-10-12', st = '2021-09-19') "
      + "partition(dt = '2021-10-12', st = '2021-09-18') "
      + "partition(dt = '2021-10-12', st = '2021-09-17') "
      + "partition(dt = '2021-10-12', st = '2021-09-16') "
      + "partition(dt = '2021-10-12', st = '2021-09-15') "
      + "partition(dt = '2021-10-12', st = '2021-09-14') "
      + "partition(dt = '2021-10-12', st = '2021-09-13') "
      + "partition(dt = '2021-10-12', st = '2021-09-12') ";

  private static String positiveLongCmds = negativeLongCmd + " archive";

  @Test
  public void testNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();


    for (int i = 0; i < positivesPair.length; i+=2) {
      ArchiveCommand command = ArchiveCommand.parse(positivesPair[i], context);
      Assert.assertNotNull(command);
      Assert.assertEquals(positivesPair[i+1].trim(), command.getCommandText().trim());
    }

    for (String cmd: negativeCmds) {
      Assert.assertNull(ArchiveCommand.parse(cmd, context));
    }

    Assert.assertNotNull(ArchiveCommand.parse(positiveLongCmds, context));
    Assert.assertNull(ArchiveCommand.parse(negativeLongCmd, context));

  }


}
