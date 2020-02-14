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

package com.aliyun.openservices.odps.console.cupid.common;

import java.util.Arrays;

public class YarnConstants {
    public enum AppState {
        NEW,
        NEW_SAVING,
        SUBMITTED,
        ACCEPTED,
        RUNNING,
        FINISHED,
        FAILED,
        KILLED
    }

    public enum FinalApplicationStatus {
        UNDEFINED,
        SUCCEEDED,
        FAILED,
        KILLED
    }

    public static Integer getAppStateCode(String state) {
        for (AppState appState : AppState.values()) {
            if (state.equalsIgnoreCase(appState.name())) {
                return appState.ordinal();
            }
        }

        throw new RuntimeException("Illegal yarn application state: " + state);
    }

    public static String getAppStateStr(long order) {
        YarnConstants.AppState[] states = YarnConstants.AppState.values();
        if (order >= states.length || order < 0) {
            throw new RuntimeException("Illegal yarn application state: " + order);
        }

        return states[(int) order].name();
    }
}
