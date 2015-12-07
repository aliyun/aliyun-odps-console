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

import static org.junit.Assert.*;

import org.apache.commons.cli.MissingArgumentException;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class ParseUploadCommandTest {

  @Before
  public void setup() throws ODPSConsoleException {
    DshipContext.INSTANCE.setExecutionContext(ExecutionContext.init());
  }

  /**
   * upload命令，表名包括project.table/partition格式<br/>
   * 命令： upload src/test/resources/test_data.txt up_test_project.test_table/ds='2113 <br/>
   **/
  @Test
  public void testProjectTablePartition() throws Exception {

    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "up_test_project.test_table/ds='2113',pt='pttest'"};

    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", "up_test_project", project);
    assertEquals("table name not equal", "test_table", table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);

    args = new String[]{"u", "src/test/resources/test_data.txt", "test_table=xxx"};
    OptionsBuilder.buildUploadOption(args);
    source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    table = DshipContext.INSTANCE.get(Constants.TABLE);
    partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", "test_table=xxx", table);
    assertEquals("partition spec not equal", null, partition);
  }

  /**
   * upload命令，表名包括project.table格式<br/>
   * 命令： upload src/test/resources/test_data.txt up_test_project.test_table<br/>
   */
  @Test
  public void testProjectTable() throws Exception {

    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "up_test_project.test_table"};

    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);

    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", "up_test_project", project);
    assertEquals("table name not equal", "test_table", table);
  }

  /**
   * upload命令，只包括table <br/>
   * 命令： upload src/test/resources/test_data.txt test_table<br/>
   */
  @Test
  public void testTable() throws Exception {

    String[] args;
    args = new String[]{"upload", "src/test/resources/test_data.txt", "test_table"};
    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", "test_table", table);
    assertEquals("partition spec not equal", null, partition);
  }

  /**
   * upload命令，包括table/partition格式 <br/>
   * 命令： upload src/test/resources/test_data.txt test_table/ds='2113',pt='pttest'<br/>
   */
  @Test
  public void testTablePartition() throws Exception {

    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'"};
    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);

    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("table name not equal", "test_table", table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);
  }

  /**
   * 测试上传文件路径包括空格和中文 <br/>
   * 命令： upload 'src/test/resources/中文space path/test_config.ini' test_project.test_table<br/>
   */
  @Test
  public void testFilePathIncludeSpaceChinese() throws Exception {
    String[] args;
    args =
        new String[]{"upload", "src/test/resources/中文space path/test_config.ini",
                     "test_project.test_table"};
    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);

    assertEquals("source not equal", "src/test/resources/中文space path/test_config.ini", source);
  }

  /**
   * 命令格式不对的测试table/partition,格式不对 <br/>
   * 命令： upload src/test/resources/test_data.txt test_project.test_table/ds/xxx <br/>
   * 命令： upload src/test/resources/test_data.txt test_project.test_table.partition <br/>
   */
  @Test
  public void testFailTablePartition() {

    String[] args;
    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_project.test_table/ds/xxx"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Invalid parameter") >= 0);
    }

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_project.test_table.partition"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Invalid parameter") >= 0);
    }
  }

  /**
   * 命令行源文件不存在 <br/>
   * 命令： upload src/test/resources/test_data_noexist.txt test_project.test_table<br/>
   */
  @Test
  public void testFileSourceFileNoExist() {
    String[] args;
    try {
      args =
          new String[]{"upload", "src/test/resources/test_data_noexist.txt",
                       "test_project.test_table"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("File not found") >= 0);
    }
  }

  /**
   * 命令行参数过多 <br/>
   * 命令： upload src/test/resources/test_data_noexist.txt
   * test_project.test_table/ds='2113',pt='pttest' badcmd<br/>
   */
  @Test
  public void testFailMoreArgs() {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "badcmd"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include bad command", e.getMessage().indexOf("Unrecognized command") >= 0);
    }
  }

  /**
   * 命令行参数过少 <br/>
   * 命令： upload src/test/resources/test_data_noexist.txt<br/>
   */
  @Test
  public void testFailLessArgs() {

    String[] args;
    try {
      args = new String[]{"upload", "src/test/resources/test_data.txt"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include bad command", e.getMessage().indexOf("Unrecognized command") >= 0);
    }
  }

  /***
   * 测试option参数正常的情况，所有option参数通过命令行传递
   */
  @Test
  public void testOptionsNormal() throws Exception {

    String[] args;
    // 普通测试
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_project.test_table/ds='2113',pt='pttest'", "-charset=gbk",
                     "-field-delimiter=||", "-record-delimiter=\r\n", "-discard-bad-records=true",
                     "-date-format-pattern=yyyy-MM-dd HH:mm:ss", "-null-indicator=NULL", "-scan=only"};

    OptionsBuilder.buildUploadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("project name not equal", "test_project",
                 DshipContext.INSTANCE.get(Constants.TABLE_PROJECT));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
    assertEquals("SCAN name not equal", "only", DshipContext.INSTANCE.get(Constants.SCAN));

  }

  /**
   * 测试--discard-bad-records的值，只能包括true|false
   */
  @Test
  public void testOptionsDiscardBadRecords() throws Exception {

    String[] args;

    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "t",
                     "-discard-bad-records=true"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("dbr", "true", DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));

    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "t",
                     "-discard-bad-records=false"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("dbr", "false", DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));

    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "t",
                     "-discard-bad-records=only"};
    try {
      OptionsBuilder.buildUploadOption(args);
      fail("need fail.");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "error message",
          e.getMessage().indexOf(
              "Invalid parameter : discard bad records expected 'true' or 'false', found 'only'") == 0);
    }
  }

  /**
   * 测试--Scan的值，只能包括true|false|only
   */
  @Test
  public void testOptionsScan() throws Exception {

    String[] args;

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-scan=true"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("scan", "true", DshipContext.INSTANCE.get(Constants.SCAN));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-scan=false"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("scan", "false", DshipContext.INSTANCE.get(Constants.SCAN));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-scan=only"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("scan", "only", DshipContext.INSTANCE.get(Constants.SCAN));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-scan=test"};
    try {
      OptionsBuilder.buildUploadOption(args);
      fail("need fail.");
    } catch (IllegalArgumentException e) {
      assertTrue("error message",
                 e.getMessage().indexOf("-scan, expected:(true|false|only), actual: 'test'") == 0);
    }
  }


  /**
   * 测试指定字符编码gbk|utf8|gb2312解析，及不支持的编码
   */
  @Test
  public void testUploadOptions_charset() throws Exception {

    String[] args;

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-charset=gbk"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("charset", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-charset=utf8"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("charset", "utf8", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-charset=gb2312"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("charset", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));

    args = new String[]{"upload", "src/test/resources/test_data.txt", "t", "-c=test"};
    try {
      OptionsBuilder.buildUploadOption(args);
      fail("need fail.");
    } catch (IllegalArgumentException e) {
      assertTrue("error message", e.getMessage().indexOf("Unsupported encoding: 'test'") == 0);
    }
  }

  /**
   * 测试命令行->配置文件->默认值的优先级顺序
   **/
  @Test
  public void testOptionsMix() throws Exception {

    String[] args;
    // 配置文件和命令行混合，级命令行优先
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-record-delimiter=\t\t",
        };
    ExecutionContext context = ExecutionContext.load("src/test/resources/test_config.ini");
    DshipContext.INSTANCE.setExecutionContext(context);
    OptionsBuilder.buildUploadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\t",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("NULL_INDICATOR name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
    assertEquals("SCAN name not equal", "only", DshipContext.INSTANCE.get(Constants.SCAN));

  }

  /**
   * 测试命令行解析, option用"="号分隔 <br/>
   * 如命令：upload src/test/resources/test_data.txt test_table -rd="\r\n"<br/>
   */
  @Test
  public void testOptionsQuote() throws Exception {

    String[] args;

    // 加引号
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_project.test_table/ds='2113',pt='pttest'", "-charset=\"gbk\"",
                     "-field-delimiter=\"||\"", "-record-delimiter=\"\r\n\"",
                     "-discard-bad-records=true", "-date-format-pattern=yyyy-MM-dd HH:mm:ss",
                     "-null-indicator=NULL", "-scan=only"};

    OptionsBuilder.buildUploadOption(args);

    assertEquals("charset not equal", "gbk", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("project name not equal", "test_project",
                 DshipContext.INSTANCE.get(Constants.TABLE_PROJECT));
    assertEquals("FIELD_DELIMITER name not equal", "||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "NULL",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
    assertEquals("SCAN name not equal", "only", DshipContext.INSTANCE.get(Constants.SCAN));

  }

  /**
   * 测试命令行解析, option用空格分隔 <br/>
   * 如：upload src/test/resources/test_data.txt test_table -rd "\r\n"<br/>
   */
  @Test
  public void testOptionssplit() throws Exception {

    String[] args;
    // 命令行逗号分隔
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-charset", "gb2312", "-field-delimiter", "|||",
                     "-record-delimiter", "\t\r\n", "-discard-bad-records", "true",
                     "-date-format-pattern", "yyyy-MM-dd HH:mm:ss", "-null-indicator", "", "-scan",
                     "false"};

    OptionsBuilder.buildUploadOption(args);

    assertEquals("charset not equal", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "|||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
    assertEquals("SCAN name not equal", "false", DshipContext.INSTANCE.get(Constants.SCAN));

  }

  /**
   * 测试短命令格式 <br/>
   * 如命令： upload src/test/resources/test_data.txt test_table -c "gb2312"<br/>
   */
  @Test
  public void testShortCommand() throws Exception {

    String[] args;

    // 短命令测试
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-c", "gb2312", "-fd", "|||", "-rd", "\t\r\n",
                     "-dbr", "true", "-dfp", "yyyy-MM-dd HH:mm:ss", "-ni", "", "-scan", "false",
        };

    OptionsBuilder.buildUploadOption(args);

    assertEquals("charset not equal", "gb2312", DshipContext.INSTANCE.get(Constants.CHARSET));
    assertEquals("FIELD_DELIMITER name not equal", "|||",
                 DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("RECORD_DELIMITER name not equal", "\t\r\n",
                 DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("DISCARD_BAD_RECORDS name not equal", "true",
                 DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    assertEquals("NULL_INDICATOR name not equal", "yyyy-MM-dd HH:mm:ss",
                 DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    assertEquals("DATE_FORMAT_PATTERN name not equal", "",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
    assertEquals("SCAN name not equal", "false", DshipContext.INSTANCE.get(Constants.SCAN));

  }

  /**
   * 测试的命令行, 命令行需要保存在context中<br/>
   */
  @Test
  public void testUploadCommandLine() throws Exception {

    // 命令行
    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "up_test_project.test_table/ds='2113',pt='pttest'", "-fd", ",", "-rd", "||"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals(
        "command not equal",
        "upload src/test/resources/test_data.txt up_test_project.test_table/ds='2113',pt='pttest' -fd , -rd ||",
        DshipContext.INSTANCE.get(Constants.COMMAND));
  }

  /**
   * 测试行分隔符、列分隔符，特殊字符\r\n\t的解析<br/>
   * 原因，命令行会把转义字符作为字符串传入 如：upload test_data.txt test_table -rd "\\r\\n"<br/>
   */
  @Test
  public void testSplitWithRNT() throws Exception {

    String[] args;
    // 普通测试
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "up_test_project.test_table/ds='2113',pt='pttest'", "-rd", "\\t\\r\\n", "-fd", "||"};

    OptionsBuilder.buildUploadOption(args);
    assertEquals("rd", "\t\r\n", DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));
    assertEquals("fd", "||", DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));

    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "test_table=xxx", "-fd",
                     "\\t\\r\\n", "-rd", "||"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("fd", "\t\r\n", DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("rd", "||", DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));

    args =
        new String[]{"upload", "src/test/resources/test_data.txt", "test_table=xxx",
                     "-fd=\\t\\r\\n", "-rd=||"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("fd", "\t\r\n", DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER));
    assertEquals("rd", "||", DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER));

  }

  /**
   * 测试不存在的option <br/>
   * 命令：upload test_data.txt test_table "--badoption=xxx"<br/>
   */
  @Test
  public void testFailMoreOption() {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-charset=gbk", "-badoption=xxx"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {

      assertTrue("need include fail messages",
                 e.getMessage().indexOf("Unrecognized option: -badoption=xxx") >= 0);
    }
  }

  /**
   * 测试列分隔符，不能够包含行分隔符，出错 <br/>
   * 命令：upload test_data.txt test_table -rd "||" -fd "||||"<br/>
   */
  @Test
  public void testFailDelimiterInclude() {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-field-delimiter", "||||",
                       "-record-delimiter", "||", "-charset=gbk"};
      OptionsBuilder.buildUploadOption(args);

      // MockUploadSession us = new MockUploadSession(context);
      // SessionHistory sh = SessionHistoryManager.createSessionHistory("xxxx");
      // FileUploader uploader = new FileUploader(new File(context.get(Constants.RESUME_PATH)), us,
      // sh);
      fail("need fail");
    } catch (Exception e) {

      assertTrue("need include bad command",
                 e.getMessage().indexOf("Field delimiter can not include record delimiter") >= 0);
    }
  }

  /**
   * 测试date format pattern设置出错 <br/>
   * 命令: upload test_data.txt test_table -dfp "abcd"<br/>
   */
  @Test
  public void testFailDataformatpattern() throws Exception {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-dfp=abcd"};
      OptionsBuilder.buildUploadOption(args);

      fail("need fail");
    } catch (IllegalArgumentException e) {

      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("Unsupported date format pattern 'abcd'") >= 0);
    }
  }

  /**
   * 测试命令行参数有缺丢的情况，如-fd=，或只有-fd<br/>
   */
  @Test
  public void testLostOption() throws Exception {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-fd"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (MissingArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Missing argument") >= 0);
    }
  }

  /**
   * 测试命令行option有大写的情况
   */
  @Test
  public void testUpperOption() throws Exception {

    String[] args;

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-Fd=,"};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {

      assertTrue(e.getMessage(), e.getMessage().indexOf("Unrecognized option") >= 0);
    }
  }

  /**
   * 测试option参数值为空字符串的情况，但NULL_INDICATOR的值可以为空串
   */
  @Test
  public void testOptionValueNullString() throws Exception {

    String[] args;
    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-fd="};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {

      assertTrue(e.getMessage(), e.getMessage().indexOf("Field delimiter is null.") >= 0);
    }

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-fd", ""};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Field delimiter is null.") >= 0);
    }

    try {
      args =
          new String[]{"upload", "src/test/resources/test_data.txt",
                       "test_table/ds='2113',pt='pttest'", "-rd", ""};
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("Record delimiter is null.") >= 0);
    }

    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-dfp", ""};
    OptionsBuilder.buildUploadOption(args);
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-ni", ""};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("NULL_INDICATOR name not equal", "",
                 DshipContext.INSTANCE.get(Constants.NULL_INDICATOR));
  }

  /**
   * 测试时区默认为空
   */
  @Test
  public void testOptionTimeZone() throws Exception {

    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("time zone null", null, DshipContext.INSTANCE.get(Constants.TIME_ZONE));

    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-tz=GMT+6"};
    OptionsBuilder.buildUploadOption(args);
    assertEquals("time zone gmt+6", "GMT+6", DshipContext.INSTANCE.get(Constants.TIME_ZONE));

  }

  /**
   * 测试指定charset有非法字符, 如引号
   */
  @Test
  public void testCharsetWithIllegalChar() throws Exception {

    String[] args;
    args =
        new String[]{"upload", "src/test/resources/test_data.txt",
                     "test_table/ds='2113',pt='pttest'", "-c='gbk'"};

    try {
      OptionsBuilder.buildUploadOption(args);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("Unsupported encoding: ''gbk''") >= 0);
    }
  }

}
