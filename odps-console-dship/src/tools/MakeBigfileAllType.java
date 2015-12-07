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

public class MakeBigfileAllType {

  public static void main(String[] args) throws Exception {
    
//    System.out.println("".length());
    
    long size = 500;
    if (args.length == 1){
      size = Long.valueOf(args[0]);
    }else{
//      return;
    }
    
    File file = new File("/Users/ganshuman/test/dship/500m_s.txt");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"),
            20 * 1024 * 1024);
    
    long l1 = 1;
    long l2 = 33;
    
//    StringBuilder sb1 = new StringBuilder();
//    StringBuilder sb2 = new StringBuilder();
//    StringBuilder sb3 = new StringBuilder();
//    StringBuilder sb4 = new StringBuilder();
//    for (int i = 0 ; i < 1 * 1024; i++){
//      sb1.append("测");
//      sb2.append("式");
//      sb3.append("双");
//      sb1.append("上");
//    }
    
//    String s1 = "Linux统计/监控工具SAR详细介绍：要判断一个系统瓶颈问题，有时需要几个 sar 命令选项结合起来使用，例如： 怀疑CPU存在瓶颈，可用 sar -u 和 sar -q deng 等来查看 怀疑内存存在瓶颈，可用 sar -B、sar -r 和 sar -W 等来查看 怀疑I/O存在瓶颈，可用 sar -b、sar -u 和 sar -d 等来查看在使用 Linux 系统时，常常会遇到各种各样的问题，比如系统容易死机或者运行速度突然变慢，这时我们常常猜测：是否硬盘空间不足，是否内存不足，是否 I/O 出现瓶颈，还是系统的核心参数出了问题？这时，我们应该考虑使用 sar 工具对系统做一个全面了解，分析系统的负载状况。sar（System Activity Reporter）是系统活动情况报告的缩写。sar 工具将对系统当前的状态进行取样，然后通过计算数据和比例来表达系统的当前运行状态。它的特点是可以连续对系统取样，获得大量的取样数据；取样数据和分析的结果都可以存入文件，所需的负载很小。 sar 是目前 Linux 上最为全面的系统性能分析工具之一，可以从多方面对系统的活动进行报告，包括：文件的读写情况、系统调用的使用情况、磁盘I/O、CPU效率、内存使用状况、进程活动及IPC有关的活动等。为了提供不同的信息，sar 提供了丰富的选项、因此使用较为复杂。sar 的命令格式使用iostat来查找占用磁盘IO较多的..abb一次linux服务器Read-only file sy..软件raid和硬件raid的区别，软raid..abb独立服务器用dd命令和vmstat命令大..abb什么是spam投诉？国外对spam如何处..abbgrub.conf内核启动失败的fallback设..abbopenvz服务器如何查看vps进程？怎样..abb为什么有些域名指向我的IP？为什么..abb在本站国外注册机构组成的域名，如..abb盛大云主机和本站的美国vps的区别abb3ware磁盘阵列raid卡tw_cli命令的一..abbLinux服务器的紧急救援模式RescueL..abbsolusvm的auto ftp backup出现错误..abb网站使用SMTP发送邮件设置服务abb网站smtp发送海外email服务，国外邮..abb从/proc/net/ip_conntrack找到异常..abb错误 kernel:System RAM resource ..abbtop发现cpu %s高居不下，cpu占用99..abb服务器 backlog limit exceeded 错..abb挂载ISO文件的Xen-HVM无法启动的故..";
    
//    String s1 = "";
    
    long total1 = l1;
    long total2 = l2;
    
    while(true){
      String s = l1 + "||" + l2+ "||20130308101010||true||" + (l1+1) + "\n";
      bw.write(s);
      if (l1%10000 == 0){
        bw.flush();
        if (file.length() > (long)size*1024*1024){
          System.out.println("lines:" + l1 + "  total1:" + total1 + "  total2:"+total2);
          bw.close();
          return;
        }
      }
      
      l1++;
      l2++;
      
      total1 += l1;
      total2 += l2;
      
    }

  }

}
