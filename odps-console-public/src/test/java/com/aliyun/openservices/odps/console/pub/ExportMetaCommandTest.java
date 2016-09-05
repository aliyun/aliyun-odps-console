package com.aliyun.openservices.odps.console.pub;

import com.aliyun.odps.*;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by nizheming on 15/10/29.
 */
public class ExportMetaCommandTest {
  private static ExecutionContext context;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    context = ExecutionContext.init();
    context.setJson(true);
    Odps odps = OdpsConnectionFactory.createOdps(context);
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c0", OdpsType.STRING));
    odps.tables().create("ExportMetaCommandTest", schema, true);
  }

  @Test
  public void testDescInJson() throws ODPSConsoleException, OdpsException {
    ExportMetaCommand command = ExportMetaCommand.parse(String.format("Desc %s.%s", context.getProjectName(), "ExportMetaCommandTest"), context);
    command.run();
  }
}
