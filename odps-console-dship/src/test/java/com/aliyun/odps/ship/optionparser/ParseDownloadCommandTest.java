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

package com.aliyun.odps.ship.optionparser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class ParseDownloadCommandTest {

  private static final String TEST_TABLE_NAME = "session_history_test";
  private static String projectName;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    projectName = context.getProjectName();
    DshipContext.INSTANCE.setExecutionContext(context);
    Odps odps = OdpsConnectionFactory.createOdps(context);

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema, true);
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TEST_TABLE_NAME, true);
  }

  /**
   * download命令，<br/>
   * 表名包括project.table/partition格式 <br/>
   * 命令：download src/test/resources/test_data.txt up_test_project.test_table/ds='2113<br/>
   **/
  @Test
  public void testProjectTablePartition() throws Exception {

    String[] args;
    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt"};

    OptionsBuilder.buildDownloadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", projectName, project);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);
  }

  /**
   * download命令，<br/>
   * 表名包括project.table格式 <br/>
   * 命令： download test_table/ds='2113  src/test/resources/test_data.txt<br/>
   */
  @Test
  public void testProjectTable() throws Exception {

    String[] args;
    args =
        new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt"};

    OptionsBuilder.buildDownloadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);

    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);
  }

  /**
   * download命令<br/>
   * 只包括table 命令： download test_table src/test/resources/test_data.txt<br/>
   */
  @Test
  public void testTable() throws Exception {

    String[] args;
    args = new String[]{"download", TEST_TABLE_NAME, "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", null, partition);
  }

  /**
   * download命令，包括table/partition格式 <br/>
   * 命令： download test_table/ds='2113',pt='pttest' src/test/resources/test_data.txt
   */
  @Test
  public void testTablePartition() throws Exception {

    String[] args;
    args =
        new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);

    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);
  }

  /**
   * 命令格式不对的测试, table/partition,格式不对 <br/>
   * 命令： download test_project.test_table/ds/xxx src/test/resources/test_data.txt<br/>
   */
  @Test
  public void testFailTablePartition() {

    String[] args;
    try {
      args =
          new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds/xxx",
                       "src/test/resources/test_data.txt"};
      OptionsBuilder.buildDownloadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Invalid table identifier"));
    }
  }

  /**
   * 命令行参数过多 <br/>
   * 命令： download src/test/resources/test_data_noexist.txt test_project.test_table/ds='2113',pt='pttest'
   * badcmd<br/>
   */
  @Test
  public void testFailMoreArgs() {

    String[] args;

    try {
      args =
          new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                       "src/test/resources/test_data.txt", "badcmd"};
      OptionsBuilder.buildDownloadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include bad command", e.getMessage().contains("Unrecognized command"));
    }
  }

  /**
   * 命令行参数过少 <br/>
   * 命令： download src/test/resources/test_data_noexist.txt<br/>
   */
  @Test
  public void testFailLessArgs() {

    String[] args;
    try {
      args = new String[]{"download", "src/test/resources/test_data.txt"};
      OptionsBuilder.buildDownloadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include bad command", e.getMessage().contains("Unrecognized command"));
    }
  }

  @Test
  public void testInstance() throws Exception {
    String[] args;
    args = new String[]{"download", "instance://123456asd", "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String instanceId = DshipContext.INSTANCE.get(Constants.INSTANE_ID);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("instanceId name not equal", "123456asd", instanceId);
    assertEquals("partition spec not equal", null, partition);
    assertEquals("table not set", null, table);

    args = new String[]{"download", "instance://test_project/123456asd", "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    instanceId = DshipContext.INSTANCE.get(Constants.INSTANE_ID);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);

    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("instanceId name not equal", "123456asd", instanceId);
    assertEquals("project not equal", "test_project", project);
  }

  @Test
  public void testInstanceIllegal() throws Exception {
    String[] args;
    args = new String[]{"download", "instance://", "test_data"};
    int count = 0;

    try {
      OptionsBuilder.buildDownloadOption(args);
    } catch (IllegalArgumentException e) {
      count++;
      assertTrue(e.getMessage().contains("Table or instanceId is null"));
    }

    args = new String[]{"download", "instance://project_name/instance/id", "test_data.txt"};
    try {
      OptionsBuilder.buildDownloadOption(args);
    } catch (IllegalArgumentException e) {
      count++;
      assertTrue(e.getMessage().contains("download instance result"));
    }

    args = new String[]{"download", "instance:///instance/", "test_data.txt"};
    try {
      OptionsBuilder.buildDownloadOption(args);
    } catch (IllegalArgumentException e) {
      count++;
      assertTrue(e.getMessage().contains("Project is empty"));
    }

    assertEquals(3, count);
  }
  /***
   * 测试option参数正常的情况，所有option参数通过命令行传递<br/>
   */
  @Test
  public void testOptionsNormal() throws Exception {

    String[] args;
    // 普通测试
    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-charset=gbk", "-field-delimiter=||",
                     "-record-delimiter=\r\n", "-date-format-pattern=yyyy-MM-dd HH:mm:ss",
                     "-null-indicator=NULL"};

    OptionsBuilder.buildDownloadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("project name not equal", projectName,
                 DshipContext.INSTANCE.get(Constants.TABLE_PROJECT));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
  }

  /**
   * 测试指定字符编码gbk|utf8|gb2312解析，及不支持的编码格式异常退出<br/>
   */
  @Test
  public void testdownloadOptions_charset() throws Exception {

    String[] args;

    args = new String[]{"download", TEST_TABLE_NAME, "src/test/resources/test_data.txt", "-charset=gbk"};
    OptionsBuilder.buildDownloadOption(args);
    assertEquals("charset", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"download", TEST_TABLE_NAME, "src/test/resources/test_data.txt", "-charset=utf8"};
    OptionsBuilder.buildDownloadOption(args);
    assertEquals("charset", "utf8", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"download", TEST_TABLE_NAME, "src/test/resources/test_data.txt", "-charset=gb2312"};
    OptionsBuilder.buildDownloadOption(args);
    assertEquals("charset", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"download", TEST_TABLE_NAME, "src/test/resources/test_data.txt", "-c=test"};
    try {
      OptionsBuilder.buildDownloadOption(args);
      fail("need fail.");
    } catch (IllegalArgumentException e) {
      assertEquals("error message", 0, e.getMessage().indexOf("Unsupported encoding: 'test'"));
    }
  }

  /**
   * 测试命令行->配置文件->默认值的优先级顺序<br/>
   **/
  @Test
  public void testOptionsMix() throws Exception {

    String[] args;
    // 配置文件和命令行混合，级命令行优先
    args =
        new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-record-delimiter=\t\t"};
    ExecutionContext context = ExecutionContext.load("src/test/resources/test_config.ini");
    DshipContext.INSTANCE.setExecutionContext(context);
    OptionsBuilder.buildDownloadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\t",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));

  }

  /**
   * 测试命令行解析, option用"="号分隔 <br/>
   * 如命令：download test_table src/test/resources/test_data.txt -rd="\r\n"<br/>
   */
  @Test
  public void testOptionsQuote() throws Exception {

    String[] args;

    // 加引号
    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-charset=\"gbk\"",
                     "-field-delimiter=\"||\"",
                     "-record-delimiter=\"\r\n\"", "-date-format-pattern=yyyy-MM-dd HH:mm:ss",
                     "-null-indicator=NULL"};

    OptionsBuilder.buildDownloadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("project name not equal", projectName,
                 DshipContext.INSTANCE.get(Constants.TABLE_PROJECT));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));

  }

  /**
   * 测试命令行解析, option用空格分隔 <br/>
   * 如：download test_table src/test/resources/test_data.txt -rd "\r\n"<br/>
   */
  @Test
  public void testOptionsSplit() throws Exception {

    String[] args;
    // 命令行逗号分隔
    args =
        new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-charset", "gb2312", "-field-delimiter",
                     "|||",
                     "-record-delimiter", "\t\r\n",
                     "-date-format-pattern", "yyyy-MM-dd HH:mm:ss", "-null-indicator", ""};

    OptionsBuilder.buildDownloadOption(args);

    assertEquals("charset not equal", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "|||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));

  }

  /**
   * 测试短命令格式 <br/>
   * 如命令： download test_table src/test/resources/test_data.txt -c "gb2312"<br/>
   */
  @Test
  public void testShortCommand() throws Exception {

    String[] args;

    // 短命令测试
    args =
        new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-c", "gb2312", "-fd", "|||", "-rd",
                     "\t\r\n",
                     "-dfp", "yyyy-MM-dd HH:mm:ss", "-ni", "",
                     };

    OptionsBuilder.buildDownloadOption(args);

    assertEquals("charset not equal", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "|||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));

  }

  /**
   * 测试的命令行，命令行需要保存在context中
   */
  @Test
  public void testdownloadCommandLine() throws Exception {

    // 命令行
    String[] args;
    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-fd", ",", "-rd", "||"};
    OptionsBuilder.buildDownloadOption(args);
    assertEquals(
        "command not equal",
        "download " + projectName + "." + TEST_TABLE_NAME +
        "/ds='2113',pt='pttest' src/test/resources/test_data.txt -fd , -rd ||",
        DshipContext.INSTANCE.get(Constants.COMMAND));
  }

  /**
   * 测试行分隔符、列分隔符，特殊字符\r\n\t的解析<br/>
   * 原因，命令行会把转义字符作为字符串传入 如：download test_table test_data.txt -rd "\\r\\n"
   */
  @Test
  public void testSplitWithRNT() throws Exception {

    String[] args;
    // 普通测试
    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-rd", "\\t\\r\\n", "-fd", "||"};

    OptionsBuilder.buildDownloadOption(args);
    assertEquals("rd", "\t\r\n", DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("fd", "||", DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));

    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                     "src/test/resources/test_data.txt", "-rd=\\t\\r\\n", "-fd=||"};

    OptionsBuilder.buildDownloadOption(args);
    assertEquals("rd", "\t\r\n", DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("fd", "||", DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
  }

  @Test
  public void testOptionsEndpoint() throws Exception {

    String[] args = new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                                 "src/test/resources/test_data.txt", "-fd", "|||", "-rd", "\t\r\n",
                                 "-tunnel_endpoint=http://dt.odps.aliyun.com"};

    OptionsBuilder.buildDownloadOption(args);

    assertEquals("FIELD_DELIMITER name not equal", "|||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("tunnel endpoint name not equal", "http://dt.odps.aliyun.com",
                 DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT));
  }

  /**
   * 测试不存在的option <br/>
   * 如命令：download test_data.txt test_table "-badoption=xxx"
   */
  @Test
  public void testFailMoreoption() {

    String[] args;

    try {
      args =
          new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                       "src/test/resources/test_data.txt", "-charset=gbk", "-badoption=xxx"};
      OptionsBuilder.buildDownloadOption(args);
      fail("need fail");
    } catch (Exception e) {

      assertTrue("need include fail messages",
                 e.getMessage().indexOf("Unrecognized option: -badoption=xxx") >= 0);
    }
  }

  /**
   * 测试列分隔符，不能够包含行分隔符，出错 <br/>
   * 如命令：download test_table test_data.txt -rd "||" -fd "||||"
   */
  @Test
  public void testFailDelimiterInclude() {

    String[] args;

    try {
      args =
          new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                       "src/test/resources/test_data.txt", "-field-delimiter", "||||",
                       "-record-delimiter", "||", "-charset=gbk"};
      OptionsBuilder.buildDownloadOption(args);

      fail("need fail");
    } catch (Exception e) {

      assertTrue("need include bad command",
                 e.getMessage().indexOf("Field delimiter can not include record delimiter") >= 0);
    }
  }

  /**
   * 测试date format pattern设置出错 <br/>
   * 如命令: download test_data.txt test_table -dfp "abcd"
   */
  @Test
  public void testFailDataformatpattern() throws Exception {

    String[] args;

    try {
      args =
          new String[]{"download", TEST_TABLE_NAME + "/ds='2113',pt='pttest'",
                       "src/test/resources/test_data.txt", "-dfp=abcd"};
      OptionsBuilder.buildDownloadOption(args);

      fail("need fail");
    } catch (IllegalArgumentException e) {

      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("Unsupported date format pattern 'abcd'") >= 0);
    }
  }

  /**
   * 测试project不为null和tableProject不为null的情况
   */
  @Test
  public void testNormalProjectAndNormalTableProject() throws Exception {
    String[] args;

    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    assertEquals("tableProject is normal", projectName, tableProject);
  }

  /**
   * 测试project为null和tableProject不为null的情况
   */
  @Test
  public void testNullProjectAndNormalTableProject() throws Exception {
    String[] args;

    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    assertEquals("tableProject is normal", projectName, tableProject);
  }

  /**
   * 测试 max-records
   */
  @Test
  public void testLimit() throws Exception {
    String[] args;

    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt", "-limit=10"};
    OptionsBuilder.buildDownloadOption(args);
    String limit = DshipContext.INSTANCE.get(Constants.LIMIT);
    assertEquals("max records is set", "10", limit);
  }

  @Test
  public void testLimitIsNull() throws Exception {
    String[] args;

    args =
        new String[]{"download", projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt"};
    OptionsBuilder.buildDownloadOption(args);
    String maxRecords = DshipContext.INSTANCE.get(Constants.LIMIT);
    assertEquals("max records is null", null, maxRecords);
  }

  @Test
  public void testLimitInvalid() throws Exception {
    String[] args;
    try {
      args = new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                          "src/test/resources/test_data.txt", "-limit=abc"};
      OptionsBuilder.buildDownloadOption(args);
      fail("lmit=abc need fail");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Invalid.") >= 0);
    }

    try {
      args =
          new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                       "src/test/resources/test_data.txt", "-limit=0"};
      OptionsBuilder.buildDownloadOption(args);
      fail("maxRows=0 need fail");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("> 0.") >= 0);
    }
  }

  @Test
  public void testSelectColumns() throws Exception {
    String[] args =
        new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt", "-ci=0,1"};
    OptionsBuilder.buildDownloadOption(args);
    Assert.assertEquals("0,1", DshipContext.INSTANCE.get(Constants.COLUMNS_INDEX));

    args =
        new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt", "-columns-index=10"};
    OptionsBuilder.buildDownloadOption(args);
    Assert.assertEquals("10", DshipContext.INSTANCE.get(Constants.COLUMNS_INDEX));

    args =
        new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt", "-cn=xy"};
    OptionsBuilder.buildDownloadOption(args);
    Assert.assertEquals("xy", DshipContext.INSTANCE.get(Constants.COLUMNS_NAME));

    args =
        new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                     "src/test/resources/test_data.txt", "-columns-name=x,y"};
    OptionsBuilder.buildDownloadOption(args);
    Assert.assertEquals("x,y", DshipContext.INSTANCE.get(Constants.COLUMNS_NAME));
  }

  @Test
  public void testSelectColumnsIllegal() throws Exception {
    try {
      String[] args =
          new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                       "src/test/resources/test_data.txt", "-ci=0,1", "-cn=x,y"};
      OptionsBuilder.buildDownloadOption(args);
      fail("Need fail: column index and column name options cannot be set together");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("these two params cannot be used together"));
    }

    try {
      String[] args =
          new String[]{"download",  projectName + "." + TEST_TABLE_NAME,
                       "src/test/resources/test_data.txt", "-ci=a,2"};
      OptionsBuilder.buildDownloadOption(args);
      fail("Need fail: column index must be int");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("columns indexes expected numeric"));
    }
  }

}
