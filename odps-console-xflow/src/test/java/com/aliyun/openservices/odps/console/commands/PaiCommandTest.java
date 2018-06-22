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

package com.aliyun.openservices.odps.console.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.xflow.PAICommand;

public class PaiCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testCNNCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    System.out.println("cnn_feature_train".toUpperCase());
    String cmd = "PAI -name cnn_feature_train -project test -Dmode=train -Dtoken=testtt"
                 + " -DtrainTableName=cnn_images_train "
                 + " -DtrainLabelColName=cid "
                 + " -DtrainImgColName=binbuf "
                 + " -DtrainImgIdColName=imgid "
                 + " -DtestTableName=cnn_images_val "
                 + " -DtestLabelColName=cid "
                 + " -DtestImgIdColName=imgid "
                 + " -DtestImgColName=binbuf "
                 + " -DtrainValProto=test/resources/new_train_val3.prototxt "
                 + " -DsolverProto=test/resources/new_solver3.prototxt";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }
  @Test
  public void testPaiCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "PAI -name=LogisticRegression  -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testPaiCostCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "PAI -name=LogisticRegression -cost -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testNullCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "PAIxxx";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNull(command);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testNegativeCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "PAI -a=b -name=LogisticRegression  -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testReplaceProperty() throws Exception {
    Method replaceMethod = PAICommand.class.getDeclaredMethod("replaceProperty", Properties.class,
                                                              String.class);
    replaceMethod.setAccessible(true);
    Properties properties = new Properties();
    properties.setProperty("inputTableName", "in");
    properties.setProperty("modelName", "model");
    replaceMethod.invoke(null, properties, "project");
    assertEquals(properties.get("inputTableName"), "project.in");
    assertEquals(properties.get("outputTableName"), null);
    assertEquals(properties.get("modelName"), "project/offlinemodels/model");
    assertEquals(properties.get("not_exist"), null);
  }

  @Test
  public void testPaiAdapter() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "pai -name=AppendId -DinputTableName=dball_bin -DoutputTableName=dball_bin_appendid -DselectedColNames=r1,blue -DIDColName=ID";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testPaiArgsMore() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "pai -name=AppendRows -DinputTableNames=dball_bin,dball_mul a=b";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testTempFile() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    URL file = this.getClass().getResource("/a.r");
    System.out.println(file);
    String cmd = "PAI -name=PAIR\n"
                 + "    -Dscript=" + file
                 + "    -Dtoken=\"v1.RHdXMVQ0QUhTTkQ0VmV3NGZOQjU1SHp5ODc0PSMxIzEjS0d1THFTRnFiVHBiNEE5OCMjMTQzMjIxNDU5OCN7IlZlcnNpb24iOiIxIiwiU3RhdGVtZW50IjpbeyJFZmZlY3QiOiJBbGxvdyIsIkFjdGlvbiI6WyJvZHBzOioiXSwiUmVzb3VyY2UiOiJhY3M6b2RwczoqOioifV19\"";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testPaiArgWithQuotes() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "PAI -name Filter -project algo_public -DoutTableName=\"pai_temp_483_4281_1\" -DinputTableName=\"test4\" -Dfilter=\"id like \\\"%1\\\"\"";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
    CommandLine cl = command.getCommandLine(cmd);

    assertEquals(cl.getOptions()[2].getValues()[0] , "outTableName");
    assertEquals(cl.getOptions()[2].getValues()[1].trim(), "pai_temp_483_4281_1");
    assertEquals(cl.getOptions()[4].getValues()[0], "filter");
    assertEquals(cl.getOptions()[4].getValues()[1].trim(), "id like \"%1\"");

    String cmd2 = "PAI -name Filter -project algo_public -DoutputColExpressions=\"case when blue==1 then \\\"yes’\\\" else \\\"no\\\" end\"";
    PAICommand command2 = PAICommand.parse(cmd, context);
    assertNotNull(command2);
    CommandLine cl2 = command.getCommandLine(cmd2);

    assertEquals(cl2.getOptions()[2].getValues()[0], "outputColExpressions");
    assertEquals(cl2.getOptions()[2].getValues()[1].trim(), "case when blue==1 then \"yes’\" else \"no\" end");

    String cmd3 = "PAI -name Filter -project algo_public -DoutputColExpressions=\"case when blue==1 then 'yes\\\"' else 'no' end\"";
    PAICommand command3 = PAICommand.parse(cmd, context);
    assertNotNull(command3);
    CommandLine cl3 = command.getCommandLine(cmd3);

    assertEquals(cl3.getOptions()[2].getValues()[0], "outputColExpressions");
    assertEquals(cl3.getOptions()[2].getValues()[1].trim(),
                 "case when blue==1 then 'yes\"' else 'no' end");

    String cmd4 = "PAI -name Filter -project algo_public -Da=\"b=c\"";
    PAICommand command4 = PAICommand.parse(cmd, context);
    assertNotNull(command4);
    CommandLine cl4 = command.getCommandLine(cmd4);

    assertEquals(cl4.getOptions()[2].getValues()[0], "a");
    assertEquals(cl4.getOptions()[2].getValues()[1].trim(), "b=c");
  }
}
