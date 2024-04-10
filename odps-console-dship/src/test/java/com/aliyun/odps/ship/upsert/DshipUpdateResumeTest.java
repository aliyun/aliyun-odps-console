package com.aliyun.odps.ship.upsert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.ParseException;
import org.junit.*;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.ship.upload.BlockUploader;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class DshipUpdateResumeTest {

  private static final String TEST_TABLE_NAME = "upsert_resume_test";
  private String projectName;
  private ExecutionContext context;

  private SessionHistory sessionHistory;

  private DshipUpdate upserter;

  private Odps odps;
  private boolean skipBefore45 = false;

  @Before
  public void setUp() throws Exception {
    context = ExecutionContext.init();
    projectName = context.getProjectName();
    DshipContext.INSTANCE.setExecutionContext(context);
    odps = OdpsConnectionFactory.createOdps(context);

    Instance i = SQLTask.run(odps, "select version();");
    i.waitForSuccess();
    String versionText = (String) SQLTask.getResult(i).get(0).get(0);
    Pattern pattern = Pattern.compile(".*ODPS_BUILD_NAME: V([0-9]+).*", Pattern.DOTALL);
    Matcher showMatcher = pattern.matcher(versionText);
    if (showMatcher.matches()) {
      int version = Integer.parseInt(showMatcher.group(1));
      if (version <= 45) {
        skipBefore45 = true;
        return;
      }
    }


    String taskName = "SqlTask";
    String
        sql =
        "create table " + TEST_TABLE_NAME
        + "(key bigint not null, value string, primary key(key)) tblproperties (\"transactional\"=\"true\");";
    System.out.println(sql);
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);

    Instance instance = odps.instances().create(task);
    System.out.println(odps.logview().generateLogView(instance, 8));
    instance.waitForSuccess();

    Map<String, String> resultMap = instance.getTaskResults();
    String result = resultMap.get(taskName);

    Instance.TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
    if (Instance.TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
      throw new Exception(result);
    }

    String[] args =
        new String[]{"upsert",
                     "src/test/resources/file/fileupserter/simple.txt",
                     projectName + "." + TEST_TABLE_NAME, "-fd=||",
                     "-rd=\n",
                     "-acp=true",
                     "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildUpsertOption(args);
    TunnelUpsertSession tunnelUpsertSession = new TunnelUpsertSession();

    String sessionId = tunnelUpsertSession.getSessionId();
    DshipContext.INSTANCE.put(Constants.RESUME_UPSERT_ID, sessionId);
    sessionHistory = SessionHistoryManager.createSessionHistory(sessionId);
    sessionHistory.saveContext();
  }

  @After
  public void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TEST_TABLE_NAME, true);
  }

  @Test
  public void testUpload() throws ODPSConsoleException, OdpsException, IOException, ParseException {
    if (skipBefore45) {
      return;
    }

    TunnelUpsertSession tunnelUpsertSession = new TunnelUpsertSession();
    String
        blockInfo =
        "1:0:156:src/test/resources/file/fileupserter/simple.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, tunnelUpsertSession, sessionHistory);
    blockUploader.upload();

    tunnelUpsertSession = new TunnelUpsertSession();
    blockInfo =
        "1:0:156:src/test/resources/file/fileupserter/block2.txt";
    block.parse(blockInfo);
    blockUploader = new BlockUploader(block, tunnelUpsertSession, sessionHistory);
    blockUploader.upload();

    tunnelUpsertSession.complete(Collections.emptyList());

    String taskName = "SqlTask";
    String sql = "select * from " + projectName + "." + TEST_TABLE_NAME + ";";
    System.out.println(sql);
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);

    Instance instance = odps.instances().create(task);
    System.out.println(odps.logview().generateLogView(instance, 8));
    instance.waitForSuccess();

    Map<String, String> resultMap = instance.getTaskResults();
    String result = resultMap.get(taskName);
    Map<String, String> map = parseStringToMap(result);
    Assert.assertTrue(map.get("1").equals("sad"));
    Assert.assertTrue(map.get("4").equals("sad"));
  }

  private Map<String, String> parseStringToMap(String input) {
    Map<String, String> map = new HashMap<>();
    String[] lines = input.split("\n");
    for (int i = 1; i < lines.length; i++) {
      String[] keyValue = lines[i].split(",");
      String key = keyValue[0].replaceAll("\"", "");
      String value = keyValue[1].replaceAll("\"", "");
      map.put(key, value);
    }
    return map;
  }


}
