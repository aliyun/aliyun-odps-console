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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Functions;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.Resources;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * 导出project的meta信息, 验证mata信息
 * 
 * @author shuman.gansm
 * */
public class ExportProjectUtil {

  /**
   * 导出project的meta信息,并生成console对应的语句
   * **/
  public static void exportProject(Odps odps, String localPath, ExecutionContext sessionContext,
      String options) throws ODPSConsoleException, OdpsException {

    File file = new File(localPath);

    if (!file.exists()) {
      file.mkdirs();
    }

    file = new File(localPath + "/resources");
    if (!file.exists()) {
      file.mkdir();
    }

    // 把sql和其它命令的分成两个文件
    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder othersBuilder = new StringBuilder();
    sqlBuilder.append(exportTables(odps, localPath, sessionContext, options));

    StringBuilder addResourceBuilder = new StringBuilder();
    StringBuilder dropResourceBuilder = new StringBuilder();
    StringBuilder createFunctionBuilder = new StringBuilder();
    StringBuilder dropFunctionBuilder = new StringBuilder();

    // 指定导出resource
    if (options.toUpperCase().contains("R")) {
      exportResources(odps, localPath, sessionContext, addResourceBuilder, dropResourceBuilder);
    }

    if (options.toUpperCase().contains("F")) {
      exportFunctions(odps, localPath, sessionContext, createFunctionBuilder, dropFunctionBuilder);
    }

    // 先删除function
    if (options.toUpperCase().contains("D")) {
      othersBuilder.append(dropFunctionBuilder);
      othersBuilder.append(dropResourceBuilder);
    }

    // 先建resource
    othersBuilder.append(addResourceBuilder);
    othersBuilder.append(createFunctionBuilder);

    try {

      ByteArrayInputStream bis = new ByteArrayInputStream(sqlBuilder.toString().getBytes());
      FileUtil.saveInputStreamToFile(bis, localPath + "/project_odps_sqls.dump");
      bis.close();

      bis = new ByteArrayInputStream(othersBuilder.toString().getBytes());
      FileUtil.saveInputStreamToFile(bis, localPath + "/project_odps_others.dump");
      bis.close();
    } catch (Exception e) {

      e.printStackTrace();
    }

  }

  private static StringBuilder exportTables(Odps odps, String localPath,
      ExecutionContext sessionContext, String options) throws ODPSConsoleException, OdpsException {

    Tables tables = odps.tables();
    List<Table> tableList = new ArrayList<Table>();
    for (Table table : tables) {
      tableList.add(table);
    }
    StringBuilder resultBuilder = new StringBuilder();

    sessionContext.getOutputWriter().writeResult("----Total tables:" + tableList.size() + "\r\n");
    resultBuilder.append("----Total tables:" + tableList.size() + "\r\n");

    if (options.toUpperCase().contains("D")) {
      // generate drop tables
      for (Table table : tables) {

        // 生成drop语句，加上if exists，删除不会出错
        String dropStr = "drop table if exists `" + table.getName() + "`;\r\n";
        sessionContext.getOutputWriter().writeResult(dropStr);
        resultBuilder.append(dropStr);
      }
    }

    for (Table tableInfo : tables) {

      String tableName = tableInfo.getName();
      // temp文件不导出
      if (tableName.startsWith("tmp_table_for_select")) {
        continue;
      }

      StringBuilder sqlBuilder = getDdlFromMeta(odps, tableInfo, options);
      if (!"".equals(sqlBuilder.toString())) {
        sessionContext.getOutputWriter().writeResult(sqlBuilder.toString());
      }

      resultBuilder.append(sqlBuilder);
    }

    return resultBuilder;
  }

  /**
   * 导出一个表的建表语句,也会为在云端用
   * */
  public static StringBuilder getDdlFromMeta(Odps odps, Table table, String options)
      throws OdpsException {

    StringBuilder sqlBuilder = new StringBuilder();

    if (options.toUpperCase().contains("T")) {
      sqlBuilder.append(getTableDdl(table));
    }

    if (options.toUpperCase().contains("P")) {

      sqlBuilder.append(getAddPartitionDdl(table));
    }

    return sqlBuilder;
  }

  private static StringBuilder getAddPartitionDdl(Table table) throws OdpsException {
    StringBuilder addPartitionBuilder = new StringBuilder();

    if (table.getSchema().getPartitionColumns().size() == 0) {
      return addPartitionBuilder;
    }

    List<Partition> partionSpecList = table.getPartitions();

    for (Partition partition : partionSpecList) {
      String partitionSpec = partition.getPartitionSpec().toString();
      if (partitionSpec.indexOf("__HIVE_DEFAULT_PARTITION__") >= 0) {
        addPartitionBuilder.append("--alter table `").append(table.getName())
            .append("` add IF NOT EXISTS partition(").append(partitionSpec).append(");\r\n");
      } else {
        addPartitionBuilder.append("alter table `").append(table.getName())
            .append("` add IF NOT EXISTS partition(").append(partitionSpec).append(");\r\n");
      }

    }

    return addPartitionBuilder;
  }

  private static StringBuilder getTableDdl(Table table) throws OdpsException {

    StringBuilder sqlBuilder = new StringBuilder();

    // 支持view的情况
    if (table.isVirtualView()) {

      String viewText = table.getViewText().replaceAll("\n", " ");

      sqlBuilder.append("drop view if exists `").append(table.getName()).append("`;\r\n");
      sqlBuilder.append("create view IF NOT EXISTS `").append(table.getName()).append("` as ")
          .append(viewText).append(";\r\n");

      return sqlBuilder;
    } else {
      sqlBuilder.append("create table IF NOT EXISTS `").append(table.getName()).append("` (");
    }

    TableSchema schema = table.getSchema();

    for (int i = 0; i < schema.getColumns().size(); ++i) {
      Column column = schema.getColumn(i);

      String name = column.getName();
      String type = column.getTypeInfo().toString().toLowerCase();

      sqlBuilder.append("`" + name + "`").append(" ").append(type);

      if (column.getComment() != null) {
        String comment = column.getComment();

        // 把双引号转意
        if (comment.indexOf("\"") > 0) {
          comment = comment.replaceAll("\"", "\\\\\"");
        }

        sqlBuilder.append(" comment \"" + comment + "\"");
      }

      if (i < schema.getColumns().size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    sqlBuilder.append(")");

    String tableComment = table.getComment();
    // 把双引号转意
    if (tableComment.indexOf("\"") > 0) {
      tableComment = tableComment.replaceAll("\"", "\\\\\"");
    }

    sqlBuilder.append(" comment \"" + tableComment + "\"");

    List<Column> partitionColumns = schema.getPartitionColumns();
    if (partitionColumns.size() > 0) {
      sqlBuilder.append(" partitioned by(");
    }

    for (int i = 0; i < partitionColumns.size(); i++) {
      Column jsPartionKey = partitionColumns.get(i);

      String name = jsPartionKey.getName();
      String type = jsPartionKey.getTypeInfo().toString().toLowerCase();

      sqlBuilder.append(name).append(" ").append(type);

      if (jsPartionKey.getComment() != null) {
        String comment = jsPartionKey.getComment();
        // 把双引号转意
        if (comment.indexOf("\"") > 0) {
          comment = comment.replaceAll("\"", "\\\\\"");
        }

        sqlBuilder.append(" comment \"" + comment + "\"");

      }

      if (i < partitionColumns.size() - 1) {
        sqlBuilder.append(", ");
      }
    }
    if (partitionColumns.size() > 0) {
      sqlBuilder.append(")");
    }

    sqlBuilder.append(getClusterClause(table));

    sqlBuilder.append(";\r\n");
    return sqlBuilder;
  }

  private static StringBuilder getClusterClause(Table table) {
    StringBuilder sb = new StringBuilder();

    if (table.getClusterInfo() != null) {
      Table.ClusterInfo clusterInfo = table.getClusterInfo();

      if (!CollectionUtils.isEmpty(clusterInfo.getClusterCols())) {
        String type = clusterInfo.getClusterType();
        if (!StringUtils.isNullOrEmpty(type) && !type.equalsIgnoreCase("HASH")) {
          sb.append(" " + type);
        }

        sb.append(" clustered by( ");
        sb.append(StringUtils.join(clusterInfo.getClusterCols().toArray(), ", "));
        sb.append(" )");
      }

      if (!CollectionUtils.isEmpty(clusterInfo.getSortCols())) {
        sb.append(" sorted by( " + StringUtils.join(clusterInfo.getSortCols().toArray(), ", "));
        sb.append(" )");
      }

      if (clusterInfo.getBucketNum() > 0) {
        sb.append(" into " + clusterInfo.getBucketNum() + " buckets");
      }

    }

    return sb;
  }

  private static void exportResources(Odps odps, String localPath, ExecutionContext sessionContext,
      StringBuilder addResourceBuilder, StringBuilder dropResourceBuilder)
      throws ODPSConsoleException, OdpsException {

    Resources resourceMetaList = odps.resources();
    List<Resource> resourceList = new ArrayList<Resource>();
    for (Resource resource : resourceMetaList) {
      resourceList.add(resource);
    }

    sessionContext.getOutputWriter().writeResult(
        "----Total resources:" + resourceList.size() + "\r\n");
    addResourceBuilder.append("----Total resources:" + resourceList.size() + "\r\n");

    for (Resource resourceMeta : resourceList) {
      Type type = resourceMeta.getType();

      StringBuilder resourceCommandBuf = new StringBuilder();

      if (resourceMeta.getName() == null || resourceMeta.getName().equals("")) {
        addResourceBuilder.append("---Failed: no resource name---:" + type + "\r\n");
        continue;
      }

      String alias = resourceMeta.getName();

      // File name, 为了避免文件名冲突，使用alias作为文件名
      String filePath = localPath + "/resources/" + alias;

      // 要注意文件名和alias关系, file和ARCHIVE的情况是用户可以指定alias名称的
      String filename = resourceMeta.getName();
      switch (type) {
      case FILE:
        if (filename != null && !filename.trim().equals("")) {
          int index1 = filename.lastIndexOf("/");
          int index2 = filename.lastIndexOf("\\");
          // file的情况把alias和文件名作为后缀
          filePath = filePath + "_" + filename.substring((index1 > index2 ? index1 : index2) + 1);
        }
        resourceCommandBuf.append("add file ").append(filePath).append(" as ").append(alias);
      break;
      case ARCHIVE:
        if (filename != null && !filename.trim().equals("")) {
          int index1 = filename.lastIndexOf("/");
          int index2 = filename.lastIndexOf("\\");
          // file的情况把alias和文件名作为后缀
          filePath = filePath + "_" + filename.substring((index1 > index2 ? index1 : index2) + 1);
        }
        resourceCommandBuf.append("add archive ").append(filePath).append(" as ").append(alias);
      break;
      case TABLE:
        // head meta
        TableResource tableResourceMeta = (TableResource) resourceMeta;
        resourceCommandBuf.append("add table ").append(tableResourceMeta.getSourceTable().getName())
            .append(" as ").append(alias);
      break;
      case PY:
        resourceCommandBuf.append("add py ").append(filePath);
      break;
      case JAR:
        resourceCommandBuf.append("add jar ").append(filePath);
      break;

      }

      String comment = resourceMeta.getComment();
      if (comment == null || comment.trim().equals("")) {
        resourceCommandBuf.append(";\r\n");
      } else {
        resourceCommandBuf.append(" comment ").append(comment).append(";\r\n");
      }

      boolean resourceExist = true;
      // download resource

      if (!resourceMeta.getType().equals(Type.TABLE)) {

        try {
          FileResource fileResource = (FileResource) resourceMeta;
          InputStream inputStream = odps.resources().getResourceAsStream(fileResource.getProject(),
              fileResource.getName());

          try {
            FileUtil.saveInputStreamToFile(inputStream, filePath);
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            try {
              inputStream.close();
            } catch (IOException e) {
              // do nothing
            }
          }
        } catch (Exception e) {

          resourceExist = false;
          sessionContext.getOutputWriter().writeResult(
              "----Failed: can't find resource:" + resourceMeta.getName() + "\r\n");
          addResourceBuilder.append("----Failed: can't find resource:" + resourceMeta.getName()
              + "\r\n"); // just print stack , don't break;
          e.printStackTrace();
        }
      }

      if (resourceExist) {
        sessionContext.getOutputWriter().writeResult("drop resource " + alias + ";\r\n");
        dropResourceBuilder.append("drop resource " + alias + ";\r\n");

        if (!"".equals(resourceCommandBuf.toString())) {
          sessionContext.getOutputWriter().writeResult(resourceCommandBuf.toString());
        }

        addResourceBuilder.append(resourceCommandBuf);
      }

    }

  }

  private static void exportFunctions(Odps odps, String localPath, ExecutionContext sessionContext,
      StringBuilder createFunctionBuilder, StringBuilder dropFunctionBuilder)
      throws ODPSConsoleException, OdpsException {

    Functions functions = odps.functions();
    List<Function> functionList = new ArrayList<Function>();
    for (Function function : functions) {
      functionList.add(function);
    }

    sessionContext.getOutputWriter().writeResult(
        "----Total functions:" + functionList.size() + "\r\n");
    createFunctionBuilder.append("----Total functions:" + functionList.size() + "\r\n");

    for (Function function : functionList) {

      StringBuilder functionCommandBuf = new StringBuilder();

      functionCommandBuf.append("create function ").append(function.getName()).append(" as '")
          .append(function.getClassPath()).append("' using '");

      for (Iterator<Resource> it = function.getResources().iterator(); it.hasNext();) {
        Resource resStr = it.next();
        functionCommandBuf.append(resStr.getName());
        if (it.hasNext()) {
          functionCommandBuf.append(",");
        }
      }

      functionCommandBuf.append("';\r\n");

      sessionContext.getOutputWriter().writeResult("drop function " + function.getName() + ";\r\n");
      dropFunctionBuilder.append("drop function " + function.getName() + ";\r\n");

      if (!"".equals(functionCommandBuf.toString())) {
        sessionContext.getOutputWriter().writeResult(functionCommandBuf.toString());
      }

      createFunctionBuilder.append(functionCommandBuf);
    }
  }

}
