package com.aliyun.openservices.odps.console.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

public class Coordinate {

  public enum CoordinateType {
    /**
     * interpret a/a.b/a.b.c => a.b.c
     * eg: desc resource a.b.c
     */
    ABC,
    /**
     * interpret a/a.b/ => a.b
     * eg: list xxx in a.b
     */
    AB,
    /**
     * set value in parse, not runtime
     * eg: drop function f -p p
     */
    FINAL
  }

  private String projectName;
  private String schemaName;
  private String objectName;

  private String partitionSpec;
  private String[] coordinate;
  private CoordinateType type;
  private int len;

  public Coordinate(String[] coordinate, CoordinateType type) throws ODPSConsoleException {
    this(coordinate, type, null);
  }

  public Coordinate(String[] coordinate, CoordinateType type, String partitionSpec)
      throws ODPSConsoleException {
    this.coordinate = coordinate;
    this.type = type;
    this.len = coordinate.length;
    if (!checkLen()) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }
    this.partitionSpec = partitionSpec;
  }

  private boolean checkLen() {
    if (CoordinateType.AB.equals(type)) {
      return len >= 0 && len <= 2;
    } else {
      return len >= 0 && len <= 3;
    }
  }

  public Coordinate(String project, String schema, String object, String partitionSpec) {
    this.projectName = project;
    this.schemaName = schema;
    this.objectName = object;
    this.partitionSpec = partitionSpec;
    this.type = CoordinateType.FINAL;
  }

  public void interpretByCtx(ExecutionContext ctx) throws ODPSConsoleException {
    if (CoordinateType.AB.equals(type)) {
      interpretAB(ctx);
    } else if (CoordinateType.ABC.equals(type)) {
      interpretABC(ctx);
    } else if (CoordinateType.FINAL.equals(type)) {
      return;
    }
  }

  private void interpretAB(ExecutionContext ctx) throws ODPSConsoleException {
    if (len == 0) {
      projectName = ODPSConsoleUtils.getDefaultProject(ctx);
      schemaName = ODPSConsoleUtils.getDefaultSchema(ctx);
    } else if (len == 1) {
      if (ctx.isProjectMode()) {
        projectName = coordinate[0];
        schemaName = null;
      } else {
        projectName = ODPSConsoleUtils.getDefaultProject(ctx);
        schemaName = coordinate[0];
      }
    } else if (len == 2) {
      projectName = coordinate[0];
      schemaName = coordinate[1];
    }
    ctx.getOutputWriter().writeDebug(projectName + "." + schemaName);
  }

  private void interpretABC(ExecutionContext ctx) throws ODPSConsoleException {
    if (coordinate.length == 1) {
      projectName = ODPSConsoleUtils.getDefaultProject(ctx);
      schemaName = ODPSConsoleUtils.getDefaultSchema(ctx);
      objectName = coordinate[0];
    } else if (coordinate.length == 2) {
      if (ctx.isProjectMode()) {
        projectName = coordinate[0];
        schemaName = ODPSConsoleUtils.getDefaultSchema(ctx);
      } else {
        projectName = ODPSConsoleUtils.getDefaultProject(ctx);
        schemaName = coordinate[0];
      }
      objectName = coordinate[1];
    } else if (coordinate.length == 3) {
      if (ctx.isProjectMode()) {
        throw new ODPSConsoleException(
            ODPSConsoleConstants.BAD_COMMAND
            + "set odps.namespace.schema=true if you want to use <project.schema.object> syntax.");
      }
      projectName = coordinate[0];
      schemaName = coordinate[1];
      objectName = coordinate[2];
    }
    ctx.getOutputWriter().writeDebug(projectName + "." + schemaName + "." + objectName);
  }

  public void setPartitionSpec(String partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  public void setProject(String project) {
    this.projectName = project;
  }

  public String getProjectName() {
    return projectName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getDisplaySchemaName() {
    // schemaName == null, displaySchemaName == default
    if (org.apache.commons.lang.StringUtils.isBlank(schemaName)) {
      return "default";
    } else {
      return schemaName;
    }
  }

  public String getObjectName() {
    return objectName;
  }

  public String getPartitionSpec() {
    return partitionSpec;
  }

  /*
   *   command and usage:
   *     - ABC command       => desc a.b.c|a.c|c
   *     - AB command        => list a.b|b
   *     - -P command        => list xxx -p projectA b
   *     - Table command     => desc (ABC command) partition(xxx)
   *     - -p Table command  => desc -p projectA (ABC command) (partition spec)
   */

  /**
   * table\\s+partition(partitionSpec)
   */
  public static final Pattern TABLE_PATTERN = Pattern.compile(
      "\\s*((?<table>[\\w.]+)(\\s*|(\\s+PARTITION\\s*\\((?<partition>.*)\\)))\\s*)");
  /**
   * table\\s+(partitionSpec)
   */
  public static final Pattern PUB_TABLE_PATTERN = Pattern.compile(
      "\\s*((?<table>[\\w.]+)(\\s*|(\\s*\\((?<partition>.*)\\)))\\s*)", Pattern.CASE_INSENSITIVE);
  public static final String TABLE_GROUP = "table";
  public static final String PARTITION_GROUP = "partition";

  public static Coordinate getCoordinateOptionP(String project, String object)
      throws ODPSConsoleException {
    // cmd: -p project object
    //      -p project
    // project might = null
    if (org.apache.commons.lang.StringUtils.isBlank(project)) {
      // interpret in runtime
      return Coordinate.getCoordinateABC(object);
    }
    // interpret now in parse
    return new Coordinate(project, null, object, null);
  }

  public static Coordinate getCoordinateAB(String listCoordinate)
      throws ODPSConsoleException {
    // AB cmd |   parts     | flag | default pj | default schema | project | schema |
    // ------ | ----------  | ---- | ---------- | -------------- | ------- | ------ |
    // null   |   []        | true |    p       |       s        |    p    |    s   |
    // null   |   []        | false|    p       |       s        |    p    |  null  |
    // a      |   [a]       | true |    p       |       s        |    p    |   a    |
    // a      |   [a]       | false|    p       |       s        |    a    |  null  |
    // a.b    |   [a, b]    | *    |    *       |       *        |    a    |   b    |
    //
    // AB = a. | .b | a.b.c is not allowed
    String[] parts = new String[0];
    if (!StringUtils.isNullOrEmpty(listCoordinate)) {
      parts = listCoordinate.split("\\.");
      for (String p : parts) {
        if (StringUtils.isBlank(p)) {
          throw new ODPSConsoleException("Illegal list coordinate: " + listCoordinate);
        }
      }
    }
    return new Coordinate(parts, CoordinateType.AB);
  }

  public static Coordinate getCoordinateABC(String cmd)
      throws ODPSConsoleException {
    return getCoordinateABC(cmd, ".");
  }

  public static Coordinate getCoordinateABC(String cmd, String sp)
      throws ODPSConsoleException {
    // ABC cmd|   parts     | flag | default pj | default schema | project | schema | object |
    // ------ | ----------  | ---- | ---------- | -------------- | ------- | ------ | ------ |
    // a      |  [a]        | true |    p       |       s        |    p    |   s    |   a    |
    // a      |  [a]        | false|    p       |       s        |    p    |  null  |   a    |
    // a.b    |  [a, b]     | true |    p       |       s        |    p    |   a    |   b    |
    // a.b    |  [a, b]     | false|    p       |       s        |    a    |  null  |   b    |
    // a.b.c  |  [a, b, c]  | true |    *       |       *        |    a    |  b     |   c    |
    //
    // ABC = null | a. | .b | a.b. | .b.c | a..b | a.b.c.d is not allowed
    // flag = false, a.b.c is not allowed

    cmd = cmd.trim();
    if (StringUtils.isNullOrEmpty(cmd)) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Empty coordinate is not allowed");
    }
    if (cmd.startsWith(sp) || cmd.endsWith(sp) || cmd.contains(sp + sp)) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Illegal coordinate: " + cmd);
    }

    if (sp == ".") {
      sp = "\\.";
    }
    String[] parts = cmd.split(sp);
    return new Coordinate(parts, CoordinateType.ABC);
  }

  public static List<Coordinate> getCoordinateRes(List<String> resList)
      throws ODPSConsoleException {
    // res
    // project(schema)/resources/res
    // project/schemas/schema/resources/res
    List<Coordinate> list = new ArrayList<>();
    for (String res: resList) {
      String[] items = res.split("/");
      if (items.length == 1) {
        list.add(new Coordinate(items, CoordinateType.ABC));
      } else if (items.length == 3) {
        String[] pureItems = new String[]{items[0], items[2]};
        list.add(new Coordinate(pureItems, CoordinateType.ABC));
      } else if (items.length == 5) {
        String[] pureItems = new String[]{items[0], items[2], items[4]};
        list.add(new Coordinate(pureItems, CoordinateType.ABC));
      }
    }
    return list;
  }

  public static Coordinate getTableCoordinate(Matcher m, ExecutionContext ctx)
      throws ODPSConsoleException {
    String table = m.group(TABLE_GROUP);
    String partitionSpec = m.group(PARTITION_GROUP);
    Coordinate coordinate = getCoordinateABC(table);
    coordinate.setPartitionSpec(partitionSpec);
    return coordinate;
  }

  /**
   * get coordinate from cmd "-p project table (partition spec)"
   *
   * @param cmdTxt     [prefix] table (partition spec)?
   * @param ctx        ExecutionContext
   * @param supportABC support project.schema.table
   * @return table coordinate
   * @throws Exception
   */
  public static Coordinate getPubTableCoordinate(String cmdTxt, ExecutionContext ctx,
                                                 boolean supportABC)
      throws Exception {
    CommandWithOptionP cmd = new CommandWithOptionP(cmdTxt);
    String project = cmd.getProjectValue();
    cmdTxt = cmd.getCmd();

    Matcher m = PUB_TABLE_PATTERN.matcher(cmdTxt);
    boolean match = m.matches();
    if (!match) {
      throw new Exception("Invalid syntax");
    }

    String table = m.group(TABLE_GROUP);
    if (!supportABC && table.contains(".")) {
      throw new Exception("Legacy cmd not support project.schema.object grammar");
    }
    String partitionSpec = m.group(PARTITION_GROUP);

    Coordinate coordinate = getCoordinateOptionP(project, table);
    coordinate.setPartitionSpec(partitionSpec);
    return coordinate;
  }

  @Override
  public String toString() {
    return "Coordinate(" + projectName + "/" + schemaName + "/"
           + objectName + "/" + partitionSpec + ")";
  }


}
