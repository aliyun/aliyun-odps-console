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

package com.aliyun.odps.ship.history;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;

public class SessionHistoryManager {

  private static List<SessionHistory> listHistory() {

    //format of session dir is '<root_dir>/sessions/<date>/<session_id>'
    String sp = Util.getSessionBaseDir() + "/sessions";

    List<SessionHistory> ls = new ArrayList<SessionHistory>();
    File file = new File(sp);
    if (file.exists()) {
      File[] fl = file.listFiles();
      for (File f : fl) {
        if (f.isDirectory() && !f.getName().startsWith(".")) {
          File[] sfl = f.listFiles();
          for (File sf : sfl) {
            ls.add(new SessionHistory(sf.getName()));
          }
        }
      }
    } else {
      return ls;
    }

    // sort history by create time
    Collections.sort(ls, new HistoryComparator());
    return ls;
  }

  public static void showHistory(int n) throws FileNotFoundException, IOException {

    List<SessionHistory> l = listHistory();
    int size = n > l.size() ? l.size() : n;

    for (int i = 0; i < size; i++) {

      int idx = l.size() - i - 1;
      Map<String, String> ctx = l.get(idx).loadProperty();
      String status = ctx.get(Constants.STATUS);
      if (SessionStatus.success.toString().equals(status) && l.get(idx).existsBad()) {
        status = "bad";
      }

      System.err.println(l.get(idx).sid + "\t" + status + "\t'" + ctx.get(Constants.COMMAND) + "'");
    }
  }

  public static SessionHistory getLatest() throws FileNotFoundException, IOException {

    List<SessionHistory> ls = listHistory();

    if (ls.size() == 0) {
      throw new FileNotFoundException(Constants.ERROR_INDICATOR + "session not found.");
    }
    return ls.get(ls.size() - 1);
  }

  public static void purgeHistory(int n) throws FileNotFoundException, IOException {

    List<SessionHistory> s = listHistory();
    Long d = System.currentTimeMillis() - n * 3600 * 24 * 1000;
    for (SessionHistory sh : s) {
      Map<String, String> ctx = sh.loadProperty();
      String sct = ctx.get(Constants.SESSION_CREATE_TIME);
      if (Long.valueOf(sct) < d) {
        sh.delete();
        System.out.println(sh.sid);
      }
    }
  }

  public static SessionHistory createSessionHistory(String sid) throws FileNotFoundException {

    File f = new File(Util.getSessionDir(sid));
    if (!f.exists() && !f.mkdirs()) {
      throw new FileNotFoundException(Constants.ERROR_INDICATOR + "create session dir fail, dir path is " + f.getPath());
    }
    return new SessionHistory(sid);
  }

}


class HistoryComparator implements Comparator<SessionHistory> {

  public int compare(SessionHistory s1, SessionHistory s2) {
    long c1 = 0;
    long c2 = 0;
    try {
      c1 = Long.valueOf(s1.loadProperty().get(Constants.SESSION_CREATE_TIME));
    } catch (Exception e) {
    }

    try {
      c2 = Long.valueOf(s2.loadProperty().get(Constants.SESSION_CREATE_TIME));
    } catch (Exception e) {
    }
    
    if (c1 > c2) {
      return 1;
    } else if (c1 == c2) {
      return 0;
    } else {
      return -1;
    }
  }
}
