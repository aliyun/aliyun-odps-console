package com.aliyun.odps.ship.download;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DownloadViewTest {
  static String TABLE = "download_view_test_table";
  static String VIEW = "download_view_test_view";
  static String OUTPUT_DIR = DownloadPartitionTest.class.getResource("/").getPath();

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    // update table data
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    // create table
    System.out.println("Creating table " + TABLE);
    odps.tables().delete(TABLE, true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("col1", OdpsType.STRING));
    tableSchema.addColumn(new Column("col2", OdpsType.BIGINT));
    odps.tables().create(TABLE, tableSchema, true);

    // insert some data
    System.out.println("Uploading data");
    Instance i = SQLTask.run(odps, "INSERT INTO " + TABLE + " select \'hello\', 123;");
    i.waitForSuccess();
    i = SQLTask.run(odps, "INSERT INTO " + TABLE + " select \'world\', 321;");
    i.waitForSuccess();

    // create view
    System.out.println("Creating view " + VIEW);
    i = SQLTask.run(odps, "CREATE VIEW IF NOT EXISTS " + VIEW
        + " AS SELECT * FROM " + TABLE + " LIMIT 1;");
    i.waitForSuccess();
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    Instance i = SQLTask.run(odps, "DROP VIEW " + VIEW + ";");
    i.waitForSuccess();
    odps.tables().delete(TABLE);
  }

  @Test
  public void testDownloadView() throws ODPSConsoleException, OdpsException, IOException {
    ExecutionContext context = ExecutionContext.init();
    Path outputPath = Paths.get(OUTPUT_DIR, "normal.txt");
    String format = "tunnel d %s %s -cp=false";
    String cmdStr = String.format(format, VIEW, outputPath.toString());
    DShipCommand cmd = DShipCommand.parse(cmdStr, context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("hello,123\n", FileUtils.readFileToString(outputPath.toFile()));
  }

  @Test
  public void testDownloadViewWithPartitionSpec() throws IOException {
   try {
     ExecutionContext context = ExecutionContext.init();
     Path outputPath = Paths.get(OUTPUT_DIR, "normal.txt");
     String format = "tunnel d %s %s -cp=false";
     String partitionSpec = "col3=1";
     String cmdStr = String.format(format, VIEW + "/" + partitionSpec, outputPath.toString());
     DShipCommand cmd = DShipCommand.parse(cmdStr, context);
     assertNotNull(cmd);
     cmd.run();

     assertEquals("hello,123\n", FileUtils.readFileToString(outputPath.toFile()));
   } catch (ODPSConsoleException e) {
     assertTrue(e.getCause().getMessage().contains("view/partition"));
   }
  }

  @Test
  public void testUploadToView() throws IOException {
    try {
      ExecutionContext context = ExecutionContext.init();
      Path outputPath = Paths.get(OUTPUT_DIR, "normal.txt");
      String format = "tunnel u %s %s -cp=false";
      String cmdStr = String.format(format, outputPath.toString(), VIEW);
      DShipCommand cmd = DShipCommand.parse(cmdStr, context);
      assertNotNull(cmd);
      cmd.run();

      assertEquals("hello,123\n", FileUtils.readFileToString(outputPath.toFile()));
    } catch (ODPSConsoleException e) {
      assertTrue(e.getCause().getMessage().contains("upload to view"));
    }
  }
}
