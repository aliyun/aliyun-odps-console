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

package com.aliyun.openservices.odps.console.utils;

import static org.junit.Assert.*;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * Created by ximo.zy on 2015/05/07
 */
public class AntlrQuotesTest {

    /*
    测试 - 引号未闭合，
    Expected=com.aliyun.openservices.odps.console.ODPSConsoleException: Bad Command, Type "help;"(--help) or "h;"(-h) for help. token recognition error at: ''dual (s string);'
   */
    @Test
    public void testParserCommand() {
        String cmd = "create table if not exists 'dual (s string);";

        String[] cmdArr = new String[]{"create table if not exists 'dual (s string);",
                "create table if not exists dual' (s string);",
                "select * from 'dual;",
                "alter table 'dual add partition(p1=\"a1\",p2=\"a2\");",
                "drop function ximotest050701;\n" +
                        "create function ximotest050701 \n" +
                        "as 'alimama_fund_rec_n_userbase.Ad2NodeExplode' \n" +
                        "using 'alimama_fund_rec_n_userbase.py;",
                "DROP VIEW IF EXISTS 'dual;",
                "DROP OFFLINEMODEL IF EXISTS 'ximo_PAI_model_bayes_0410003;\n" +
                        "pai -name NaiveBayes\n" +
                        "-DmodelName=\"ximo_PAI_model_bayes_0410003 -DinputTableName=\"ximo_test_table_0410001\"\n" +
                        "-DlabelColName=\"blue\"\n" +
                        "-DfeatureColNames=\"r1\"\n" +
                        "-DisFeatureContinuous=\"1\";",
                "drop table if exists 'dual ;"
            };

        int count = 0;
        for (int i = 0; i < cmdArr.length; i++) {
            cmd = cmdArr[i];
            try {
                List<String> tokens = (new AntlrObject(cmd)).splitCommands();
            } catch (ODPSConsoleException e) {
                count ++;
                //e.printStackTrace();
                assertTrue(e.getMessage().contains("Bad Command"));
            }
        }

        assertEquals(count, cmdArr.length);
    }

}
