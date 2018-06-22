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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.xflow.XFlowProgressHelper;
import com.aliyun.openservices.odps.console.xflow.XFlowProgressHelperRegistry;
import com.aliyun.openservices.odps.console.xflow.XFlowStageProgressHelper;


/**
 * Created by zhenhong on 15/8/18.
 */
public class XFlowProgressHelperTest {

  @Test
  public void testCNNInstanceFetcher() throws OdpsException, ODPSConsoleException {
    String algoName = "CNN_Feature_Train";
    XFlowProgressHelper fetcher =
        XFlowProgressHelperRegistry.getProgressHelper(algoName.toUpperCase());

    Assert.assertEquals(30000, fetcher.getInterval());
    Assert.assertEquals(algoName.toUpperCase(), fetcher.getPAIAlgoName());

    Instance i = EasyMock.createMock(Instance.class);

    String
        CNNTaskDetailJsonStr =
        "{\"message\" : [{\"__source__\": \"test\",\"__time__\": \"123\",\"hostname\": \"adf0\",\"log_level\": \"INFO\",\"msg\": \"invalid json\",\"pid\": \"1\",\"src\": \"run.py:64\",\"time\": \"2015-8-11 16:40:20\"}, {\"__source__\": \"test2\",\"__time__\": \"334\",\"hostname\": \"adf67\",\"log_level\": \"INFO\",\"msg\": \"connect to oss\",\"pid\": \"1\",\"src\": \"run.py:87\",\"time\": \"2015-8-11 16:51:30\"}]}";

    EasyMock.expect(i.getTaskNames()).andReturn(
        new LinkedHashSet<String>(Arrays.asList("testTask", "testTask2")));
    EasyMock.expect(i.getTaskDetailJson2("testTask")).andReturn(CNNTaskDetailJsonStr);
    EasyMock.expect(i.getTaskDetailJson2("testTask2")).andReturn(CNNTaskDetailJsonStr);

    EasyMock.replay(i);

    String echo = fetcher.getProgressMessage(i);
    String res = "testTask : \n"
                 + "2015-8-11 16:40:20\t\tinvalid json\n"
                 + "2015-8-11 16:51:30\t\tconnect to oss\n"
                 + "\n"
                 + "testTask2 : \n"
                 + "2015-8-11 16:40:20\t\tinvalid json\n"
                 + "2015-8-11 16:51:30\t\tconnect to oss\n";
    Assert.assertEquals(echo, res);
  }

  @Test
  public void testInstanceProgressFetcher() throws ODPSConsoleException, OdpsException {
    String algoName = "MergeVertically";

    XFlowProgressHelper
        fetcher =
        XFlowProgressHelperRegistry.getProgressHelper(algoName);

    Assert.assertEquals(5000, fetcher.getInterval());
    Assert.assertTrue((fetcher instanceof XFlowStageProgressHelper));

    Instance i = EasyMock.createMock(Instance.class);

    EasyMock.expect(i.getTaskNames()).andReturn(
        new HashSet<String>(Arrays.asList("testTask")));
    EasyMock.expect(i.getTaskProgress("testTask"))
        .andReturn(new ArrayList<Instance.StageProgress>());

    EasyMock.replay(i);

    String echo = fetcher.getProgressMessage(i);
    Assert.assertEquals(echo, "testTask: running");
  }
}
