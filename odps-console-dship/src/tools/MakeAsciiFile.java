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

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class MakeAsciiFile {

  public static void main(String[] args) throws Exception {
    
    long size = 2;
    if (args.length == 1){
      size = Long.valueOf(args[0]);
    }else{
//      return;
    }
    
    File file = new File("/Users/ganshuman/myapp/odps/odps_trunk/odps_tools/odpsloader/test/data/ascii2m.txt");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"),
            20 * 1024 * 1024);
    
    long l1 = 1;
    long d1 = 33;
    
    long total1 = l1;
    long total2 = d1;
    
    while(true){
      String s = l1 + "," + l1+ ",20130308101010,true,123.12"+'\u0000'+'\n';
      bw.write(s);
      if (l1%100 == 0){
        bw.flush();
        if (file.length() > (long)size*1024*1024){
          System.out.println("lines:" + l1 + "  total1:" + total1 + "  total2:"+total2);
          bw.close();
          return;
        }
      }
      
      l1++;
      d1++;
      total1 += l1;
      total2 += d1;
    }
  }

}
