package com.aliyun.openservices.odps.console.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import junit.framework.TestCase;

public class CoordinateTest extends TestCase {

  private static final String P = "p";
  private static final String S = "s";

  private static ExecutionContext getCtx(boolean flag, String project, String schema) {
    ExecutionContext ctx = new ExecutionContext();
    ctx.setOdpsNamespaceSchema(flag);
    ctx.setProjectName(project);
    ctx.setSchemaName(schema);
    return ctx;
  }

  private static Coordinate getExpectedCoordinate(String expectedStr) {
    String[] e = expectedStr.split("\\.");
    for (int i = 0; i < e.length; i++) {
      if ("null".equals(e[i])) {
        e[i] = null;
      }
    }
    String project = e.length > 0 ? e[0] : null;
    String schema = e.length > 1 ? e[1] : null;
    String object = e.length > 2 ? e[2] : null;
    String pt = e.length > 3 ? e[3] : null;
    return new Coordinate(project, schema, object, pt);
  }

  private static void assertEquals(Coordinate expected, Coordinate result) {
    Assert.assertEquals(expected.getProjectName(), result.getProjectName());
    Assert.assertEquals(expected.getSchemaName(), result.getSchemaName());
    Assert.assertEquals(expected.getObjectName(), result.getObjectName());
    Assert.assertEquals(expected.getPartitionSpec(), result.getPartitionSpec());
  }


  static class TestItem {

    public TestItem(boolean flag, String defaultProject, String defaultSchema, String cmd,
                    String expected) {
      this.flag = flag;
      this.defaultProject = defaultProject;
      this.defaultSchema = defaultSchema;
      this.cmd = cmd;
      this.expectedStr = expected;

      this.ctx = getCtx(flag, defaultProject, defaultSchema);
      this.expected = getExpectedCoordinate(expectedStr);
    }

    public boolean flag;
    public String defaultProject;
    public String defaultSchema;
    public String cmd;
    public String expectedStr;
    public ExecutionContext ctx;
    public Coordinate expected;

    @Override
    public String toString() {
      return "TestItem{" +
             "flag=" + flag +
             ", defaultProject='" + defaultProject + '\'' +
             ", defaultSchema='" + defaultSchema + '\'' +
             ", cmd='" + cmd + '\'' +
             ", expectedStr='" + expectedStr + '\'' +
             ", expected=" + expected +
             '}';
    }
  }

  public static List<TestItem> abcTestItems;
  public static List<TestItem> abcErrorItems;
  public static List<TestItem> legacyACTestItems;
  public static List<TestItem> listTestItems;

  static {
    // flag     project     schema        cmd       result
    // false    ANY         ANY           a.b.c     a.b.c     => ERROR
    // false    p           null          a.c       a.null.c
    // false    p           s             a.c       a.null.c
    // false    p           null          c         p.null.c
    // false    p           s             c         p.null.c
    // true     ANY         ANY           a.b.c     a.b.c
    // true     p           null          b.c       p.b.c
    // true     p           s             b.c       p.b.c
    // true     p           null          c         p.null.c
    // true     p           s             c         p.s.c
    abcTestItems = new ArrayList<>(Arrays.asList(
        new TestItem(false, P, null, "a.c", "a.null.c"),
        new TestItem(false, P, S, "a.c", "a.null.c"),
        new TestItem(false, P, null, "c", "p.null.c"),
        new TestItem(false, P, S, "c", "p.null.c"),
        new TestItem(true, null, null, "a.b.c", "a.b.c"),
        new TestItem(true, P, S, "a.b.c", "a.b.c"),
        new TestItem(true, P, null, "b.c", "p.b.c"),
        new TestItem(true, P, S, "b.c", "p.b.c"),
        new TestItem(true, P, null, "c", "p.null.c"),
        new TestItem(true, P, S, "c", "p.s.c")
    ));

    abcErrorItems = new ArrayList<>(Collections.singletonList(
        new TestItem(false, P, S, "a.b.c", "a.b.c")));

    // flag     project     schema        cmd       result
    // false    p           null          a.c       a.null.c
    // false    p           s             a.c       a.null.c
    // true     p           null          null.c    p.null.c
    // true     p           s             null.c    p.s.c
    legacyACTestItems = new ArrayList<>(Arrays.asList(
        new TestItem(false, P, null, "a.c", "a.null.c"),
        new TestItem(false, P, S, "a.c", "a.null.c"),
        new TestItem(false, P, null, "null.c", "p.null.c"),
        new TestItem(false, P, S, "null.c", "p.null.c")
    ));

    // flag     project     schema        cmd       result
    // ANY      ANY         ANY           a.b       a.b
    // false    p           null                    p.null
    // false    p           s                       p.null
    // false    p           s             a         a.null
    // true     p           null                    p.null
    // true     p           s                       p.s
    // true     p           null          b         p.b
    // true     p           s             b         p.b
    listTestItems = new ArrayList<>(Arrays.asList(
        new TestItem(false, P, null, "", "p.null"),
        new TestItem(false, P, S, "", "p.null"),
        new TestItem(false, P, S, "a", "a.null"),
        new TestItem(false, P, S, "a.b", "a.b"),

        new TestItem(true, P, null, "", "p.null"),
        new TestItem(true, P, S, "", "p.s"),
        new TestItem(true, P, null, "b", "p.b"),
        new TestItem(true, P, S, "b", "p.b"),
        new TestItem(true, P, null, "a.b", "a.b"),
        new TestItem(true, P, S, "a.b", "a.b")
    ));
  }

  @Test
  public void testGetCoordinateABC() throws ODPSConsoleException {
    for (TestItem item : abcTestItems) {
      Coordinate result = Coordinate.getCoordinateABC(item.cmd);
      result.interpretByCtx(item.ctx);
      assertEquals(item.expected, result);
    }

    for (TestItem item: abcErrorItems) {
      try {
        Coordinate result = Coordinate.getCoordinateABC(item.cmd);
        result.interpretByCtx(item.ctx);
        Assert.fail("should throw exception");
      } catch (ODPSConsoleException ignored) {
      }
    }
  }

  @Test
  public void testGetCoordinateAB() throws ODPSConsoleException {
    for (TestItem item : listTestItems) {
      System.out.println(item);
      Coordinate result = Coordinate.getCoordinateAB(item.cmd);
      result.interpretByCtx(item.ctx);
      assertEquals(item.expected, result);
    }
  }

  @Test
  public void testGetLegacyCoordinate() throws ODPSConsoleException {
    for (TestItem item : legacyACTestItems) {
      String[] coor = item.cmd.split("\\.");
      if (coor[0].equals("null")) {
        coor[0] = null;
      }
      System.out.println(item);
      Coordinate result = Coordinate.getCoordinateOptionP(coor[0], coor[1]);
      result.interpretByCtx(item.ctx);
      assertEquals(item.expected, result);
    }
  }
}