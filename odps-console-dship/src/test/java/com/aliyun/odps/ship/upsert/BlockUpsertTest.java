package com.aliyun.odps.ship.upsert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.ship.upload.BlockUploader;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class BlockUpsertTest {

  private static final String TEST_TABLE_NAME = "block_upsert_test";
  private static String projectName;
  private static ExecutionContext context;

  private static Odps odps;


  @BeforeClass
  public static void setup() throws Exception {
    context = ExecutionContext.init();
    projectName = context.getProjectName();
    DshipContext.INSTANCE.setExecutionContext(context);
    odps = OdpsConnectionFactory.createOdps(context);
    String
        sql =
        "create table if not exists " + TEST_TABLE_NAME + "(key bigint not null, value string, primary key(key)) tblproperties (\"transactional\"=\"true\");";
    System.out.println(sql);

    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.upsertable.table.enable", "true");

    Instance instance = SQLTask.run(odps, projectName, sql, hints, null);
    System.out.println(odps.logview().generateLogView(instance, 8));
    instance.waitForSuccess();
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TEST_TABLE_NAME, true);
  }

  @Test
  public void testUpsertCommand() throws Exception {

    String file = this.getClass().getResource("/file/fileupserter/simple.txt").getFile();
    System.out.println(file);

    DShipCommand command = DShipCommand.parse(
        "tunnel us " + file + "  " + projectName + "." + TEST_TABLE_NAME + " -fd=||", context);
    command.run();

    String taskName = "SqlTask";
    String sql = "select value from " + projectName + "." + TEST_TABLE_NAME + " where key=1;";
    System.out.println(sql);
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);

    Instance instance = odps.instances().create(task);
    System.out.println(odps.logview().generateLogView(instance, 8));
    instance.waitForSuccess();

    Map<String, String> resultMap = instance.getTaskResults();
    String result = resultMap.get(taskName);

    Assert.assertEquals(result, "\"value\"\n\"happy\"\n");
  }


  /**
   * 测试上传一个Block成功，上传成功，检查finish block 状态符合预期
   */
  @Test
  public void testUpsertSingleFile() throws Exception {

    String[] args =
        new String[]{"upsert",
                     "src/test/resources/file/fileupserter/simple.txt",
                     projectName + "." + TEST_TABLE_NAME, "-fd=||",
                     "-rd=\n",
                     "-acp=true",
                     "-dfp=yyyyMMddHHmmss"};

    OptionsBuilder.buildUpsertOption(args);

    TunnelUpsertSession us = new TunnelUpsertSession();
    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getSessionId());
    sh.saveContext();
    sh.loadContext();
    String
        blockInfo =
        "1:0:156:src/test/resources/file/fileupserter/simple.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUpserter = new BlockUploader(block, us, sh, false);
    blockUpserter.upload();
    List<BlockInfo> blockList = sh.loadFinishBlockList();
    assertEquals("finish block is not 1", blockList.size(), 1);
    assertEquals("block id is not 1", blockList.get(0).getBlockId(), Long.valueOf(1L));
  }

  /**
   * 测试--discard-bad-records=false，不丢弃脏数据，当有脏数据时出错
   * */
  @Test
  public void testFailDiscardBadRecordsFalse() throws Exception {

    String[] args =
        new String[]{"upsert",
                     "src/test/resources/file/fileupserter/badRecord.txt",
                     projectName + "." + TEST_TABLE_NAME, "-fd=||",
                     "-rd=\n",
                     "-acp=true",
                     "--discard-bad-records=false",
                     "-dfp=yyyyMMddHHmmss"};

    OptionsBuilder.buildUploadOption(args);
    TunnelUpsertSession us = new TunnelUpsertSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getSessionId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileupserter/badRecord.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUpserter = new BlockUploader(block, us, sh, false);

    try {
      blockUpserter.upload();
      fail("don't throw exception on bad record");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("ERROR: format error") == 0);
    }
  }



}
