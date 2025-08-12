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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;
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
    String cmd
        = "PAI -name=LogisticRegression  -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testPaiCostCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd
        = "PAI -name=LogisticRegression -cost -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
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
    String cmd
        = "PAI -a=b -name=LogisticRegression  -DmodelName=odpstest_andy/offlinemodels/dball_mul1 -Dmaxiter=100 -DregularizedLevel=1 -DregularizedType=l1 -DlabelColName=blue -DEpsilon=0.000001 -DfeatureColNames=r6,r1,r2,r3,r4,week,r5 -DgoodValue=1 -DinputTableName=odpstest_andy.dball_mul";
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
    String cmd
        = "pai -name=AppendId -DinputTableName=dball_bin -DoutputTableName=dball_bin_appendid -DselectedColNames=r1,blue -DIDColName=ID";
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
    String cmd
        = "PAI -name Filter -project algo_public -DoutTableName=\"pai_temp_483_4281_1\" -DinputTableName=\"test4\" -Dfilter=\"id like \\\"%1\\\"\"";
    PAICommand command = PAICommand.parse(cmd, context);
    assertNotNull(command);
    CommandLine cl = command.getCommandLine(cmd);

    assertEquals(cl.getOptions()[2].getValues()[0], "outTableName");
    assertEquals(cl.getOptions()[2].getValues()[1].trim(), "pai_temp_483_4281_1");
    assertEquals(cl.getOptions()[4].getValues()[0], "filter");
    assertEquals(cl.getOptions()[4].getValues()[1].trim(), "id like \"%1\"");

    String cmd2
        = "PAI -name Filter -project algo_public -DoutputColExpressions=\"case when blue==1 then \\\"yes’\\\" else \\\"no\\\" end\"";
    PAICommand command2 = PAICommand.parse(cmd, context);
    assertNotNull(command2);
    CommandLine cl2 = command.getCommandLine(cmd2);

    assertEquals(cl2.getOptions()[2].getValues()[0], "outputColExpressions");
    assertEquals(cl2.getOptions()[2].getValues()[1].trim(), "case when blue==1 then \"yes’\" else \"no\" end");

    String cmd3
        = "PAI -name Filter -project algo_public -DoutputColExpressions=\"case when blue==1 then 'yes\\\"' else 'no' end\"";
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

  @Test
  public void testPaiLineageCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String validCmd1SingleQuotesWithESC = "pai -name tensorflow1120_py3  -lineage '{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/input_model/\\\"}}],\\\"DestEntities\\\":[{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/output_model/\\\"}}]}' -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\\\"ps\\\":{\\\"count\\\":1, \\\"gpu\\\":0}, \\\"worker\\\":{\\\"count\\\":1, \\\"gpu\\\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command1 = PAICommand.parse(validCmd1SingleQuotesWithESC, context);
    assertNotNull(command1);
    CommandLine cl1 = PAICommand.getCommandLine(validCmd1SingleQuotesWithESC);
    HashMap<String, String> userConfig1 = PAICommand.getUserConfig(cl1);
    assertTrue(userConfig1.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/input_model/\\\"}}],\\\"DestEntities\\\":[{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/output_model/\\\"}}]}"));


    String validCmd2DoubleQuotesWithESC = "pai -name tensorflow1120_py3  -lineage \"{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/input_model/\\\"}}],\\\"DestEntities\\\":[{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/output_model/\\\"}}]}\" -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\\\"ps\\\":{\\\"count\\\":1, \\\"gpu\\\":0}, \\\"worker\\\":{\\\"count\\\":1, \\\"gpu\\\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command2 = PAICommand.parse(validCmd2DoubleQuotesWithESC, context);
    assertNotNull(command2);
    CommandLine cl2 = PAICommand.getCommandLine(validCmd2DoubleQuotesWithESC);
    HashMap<String, String> userConfig2 = PAICommand.getUserConfig(cl2);
    assertTrue(userConfig2.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/input_model/\\\"}}],\\\"DestEntities\\\":[{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/output_model/\\\"}}]}"));

    String validCmd3SingleQuotesWithoutESC = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"maxcompute-table.project1.table1\"},{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"backet1\",\"Path\":\"/input_model/\"}}],\"DestEntities\":[{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"backet1\",\"Path\":\"/output_model/\"}}]}' -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command3 = PAICommand.parse(validCmd3SingleQuotesWithoutESC, context);
    assertNotNull(command3);
    CommandLine cl3 = PAICommand.getCommandLine(validCmd3SingleQuotesWithoutESC);
    HashMap<String, String> userConfig3 = PAICommand.getUserConfig(cl3);
    assertTrue(userConfig3.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/input_model/\\\"}}],\\\"DestEntities\\\":[{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"backet1\\\",\\\"Path\\\":\\\"/output_model/\\\"}}]}"));

    String validCmd4AllEntities = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"maxcompute-table.project1.table1\",\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"llm_bucket\",\"Path\":\"/models/\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command4 = PAICommand.parse(validCmd4AllEntities, context);
    assertNotNull(command4);
    CommandLine cl4 = PAICommand.getCommandLine(validCmd4AllEntities);
    HashMap<String, String> userConfig4 = PAICommand.getUserConfig(cl4);
    assertTrue(userConfig4.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\",\\\"Attributes\\\":{\\\"ResourceType\\\":\\\"dataset\\\",\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"maxcompute-offlinemodel.project1.model1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"Attributes\\\":{\\\"Bucket\\\":\\\"llm_bucket\\\",\\\"Path\\\":\\\"/models/\\\",\\\"ResourceType\\\":\\\"model\\\",\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"EntityType\\\":\\\"nas-file\\\",\\\"Attributes\\\":{\\\"Uri\\\":\\\"nas://0bd314b6ac.cn-hangzhou/\\\",\\\"DataSourceType\\\":\\\"NAS\\\"}}],\\\"DestEntities\\\":[{\\\"QualifiedName\\\":\\\"pai-dataset.d-k0s7kplxcs94oohsgt\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"QualifiedName\\\":\\\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\\\"}]}"));

    String invalidCmd5notJsonFormat = "pai -name tensorflow1120_py3  -lineage sssss -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\\\"ps\\\":{\\\"count\\\":1, \\\"gpu\\\":0}, \\\"worker\\\":{\\\"count\\\":1, \\\"gpu\\\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command5 = PAICommand.parse(invalidCmd5notJsonFormat, context);
    assertNotNull(command5);
    CommandLine cl5 = PAICommand.getCommandLine(invalidCmd5notJsonFormat);
    try {
      PAICommand.getUserConfig(cl5);
      fail("Expected ODPSConsoleException to be thrown");
    }catch (ODPSConsoleException e){
    }

    String invalidCmd6missingSrcEntities = "pai -name tensorflow1120_py3  -lineage '{\"DestEntities\":[{\"QualifiedName\":\"sss\"}]}' -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command6 = PAICommand.parse(invalidCmd6missingSrcEntities, context);
    assertNotNull(command6);
    CommandLine cl6 = PAICommand.getCommandLine(invalidCmd6missingSrcEntities);
    try {
      PAICommand.getUserConfig(cl6);
      fail("Expected ODPSConsoleException to be thrown");
    }catch (ODPSConsoleException e){
    }

    String validCmd7AllEntitiesWithMoreUnusedFields = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"maxcompute-table.project1.table1\",\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"foo\":\"bar\",\"Attributes\":{\"foo\":\"bar\",\"Bucket\":\"llm_bucket\",\"Path\":\"/models/\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command7 = PAICommand.parse(validCmd7AllEntitiesWithMoreUnusedFields, context);
    assertNotNull(command7);
    CommandLine cl7 = PAICommand.getCommandLine(validCmd7AllEntitiesWithMoreUnusedFields);
    HashMap<String, String> userConfig7 = PAICommand.getUserConfig(cl7);
    assertTrue(userConfig7.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"maxcompute-table.project1.table1\\\",\\\"Attributes\\\":{\\\"ResourceType\\\":\\\"dataset\\\",\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"maxcompute-offlinemodel.project1.model1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"foo\\\":\\\"bar\\\",\\\"Attributes\\\":{\\\"foo\\\":\\\"bar\\\",\\\"Bucket\\\":\\\"llm_bucket\\\",\\\"Path\\\":\\\"/models/\\\",\\\"ResourceType\\\":\\\"model\\\",\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"EntityType\\\":\\\"nas-file\\\",\\\"Attributes\\\":{\\\"Uri\\\":\\\"nas://0bd314b6ac.cn-hangzhou/\\\",\\\"DataSourceType\\\":\\\"NAS\\\"}}],\\\"DestEntities\\\":[{\\\"QualifiedName\\\":\\\"pai-dataset.d-k0s7kplxcs94oohsgt\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"QualifiedName\\\":\\\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\\\"}]}"));

    String invalidCmd8MissingMaxcomputeQualifiedName = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"llm_bucket\",\"Path\":\"/models/\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command8 = PAICommand.parse(invalidCmd8MissingMaxcomputeQualifiedName, context);
    assertNotNull(command8);
    CommandLine cl8 = PAICommand.getCommandLine(invalidCmd8MissingMaxcomputeQualifiedName);
    try {
      PAICommand.getUserConfig(cl8);
      fail("Expected ODPSConsoleException to be thrown");
    }catch (ODPSConsoleException e){
    }

    String invalidCmd9MissingOssFileAttributePath = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"maxcompute-table.project1.table1\",\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"llm_bucket\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command9 = PAICommand.parse(invalidCmd9MissingOssFileAttributePath, context);
    assertNotNull(command9);
    CommandLine cl9= PAICommand.getCommandLine(invalidCmd9MissingOssFileAttributePath);
    try {
      PAICommand.getUserConfig(cl9);
      fail("Expected ODPSConsoleException to be thrown");
    }catch (ODPSConsoleException e){
    }

    String invalidCmd10UnsupportEntityType = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"maxcompute-tab.project1.table1\",\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"Attributes\":{\"Bucket\":\"llm_bucket\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command10 = PAICommand.parse(invalidCmd10UnsupportEntityType, context);
    assertNotNull(command10);
    CommandLine cl10= PAICommand.getCommandLine(invalidCmd10UnsupportEntityType);
    try {
      PAICommand.getUserConfig(cl10);
      fail("Expected ODPSConsoleException to be thrown");
    }catch (ODPSConsoleException e){
    }


    String validCmd11CustomEntityType = "pai -name tensorflow1120_py3  -lineage '{\"SrcEntities\":[{\"QualifiedName\":\"custom-usertype.user.entity1\",\"Attributes\":{\"ResourceType\":\"dataset\",\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"maxcompute-offlinemodel.project1.model1\"},{\"EntityType\":\"oss-file\",\"foo\":\"bar\",\"Attributes\":{\"foo\":\"bar\",\"Bucket\":\"llm_bucket\",\"Path\":\"/models/\",\"ResourceType\":\"model\",\"ResourceUse\":\"base\"}},{\"EntityType\":\"nas-file\",\"Attributes\":{\"Uri\":\"nas://0bd314b6ac.cn-hangzhou/\",\"DataSourceType\":\"NAS\"}}],\"DestEntities\":[{\"QualifiedName\":\"pai-dataset.d-k0s7kplxcs94oohsgt\",\"Attributes\":{\"ResourceUse\":\"train\"}},{\"QualifiedName\":\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\"Attributes\":{\"ResourceUse\":\"base\"}},{\"QualifiedName\":\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\"}]}'  -project algo_public -Dtags='others' -Dscript='file:///Users/everettli/Desktop/main.py' -DenableDynamicCluster='true' -DjobName='ft_test' -Dcluster='{\"ps\":{\"count\":1, \"gpu\":0}, \"worker\":{\"count\":1, \"gpu\":0}}' -DuseSparseClusterSchema='false'";
    PAICommand command11 = PAICommand.parse(validCmd11CustomEntityType, context);
    assertNotNull(command11);
    CommandLine cl11 = PAICommand.getCommandLine(validCmd11CustomEntityType);
    HashMap<String, String> userConfig11 = PAICommand.getUserConfig(cl11);
    assertTrue(userConfig11.get("settings").contains("{\\\"SrcEntities\\\":[{\\\"QualifiedName\\\":\\\"custom-usertype.user.entity1\\\",\\\"Attributes\\\":{\\\"ResourceType\\\":\\\"dataset\\\",\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"maxcompute-offlinemodel.project1.model1\\\"},{\\\"EntityType\\\":\\\"oss-file\\\",\\\"foo\\\":\\\"bar\\\",\\\"Attributes\\\":{\\\"foo\\\":\\\"bar\\\",\\\"Bucket\\\":\\\"llm_bucket\\\",\\\"Path\\\":\\\"/models/\\\",\\\"ResourceType\\\":\\\"model\\\",\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"EntityType\\\":\\\"nas-file\\\",\\\"Attributes\\\":{\\\"Uri\\\":\\\"nas://0bd314b6ac.cn-hangzhou/\\\",\\\"DataSourceType\\\":\\\"NAS\\\"}}],\\\"DestEntities\\\":[{\\\"QualifiedName\\\":\\\"pai-dataset.d-k0s7kplxcs94oohsgt\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"train\\\"}},{\\\"QualifiedName\\\":\\\"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\\\",\\\"Attributes\\\":{\\\"ResourceUse\\\":\\\"base\\\"}},{\\\"QualifiedName\\\":\\\"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\\\"}]}"));
  }
}
