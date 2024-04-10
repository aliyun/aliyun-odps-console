package com.aliyun.openservices.odps.console.commands.auth;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.auth.SetProjectCommand;

public class SetProjectCommandTest {
  @Test
  public void testParseProperties() {
    String setProjectCommand = "setproject  noEquals onlyEquals= \t\t normal=123 \n space=\"hello world\" brackets={hello world} \r ";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(5, map.size());
    Assert.assertEquals("", map.get("noEquals"));
    Assert.assertEquals("", map.get("onlyEquals"));
    Assert.assertEquals("123", map.get("normal"));
    Assert.assertEquals("\"hello world\"", map.get("space"));
    Assert.assertEquals("{hello world}", map.get("brackets"));
  }

  @Test
  public void testParsePropertiesSimple1() {
    String setProjectCommand = "setproject odps.function.strictmode=true";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(1, map.size());
    Assert.assertEquals("true", map.get("odps.function.strictmode"));
  }

  @Test
  public void testParsePropertiesSimple2() {
    String setProjectCommand = "setproject odps.table.drop.ignorenonexistent=true odps.instance.priority.autoadjust=false odps.instance.priority.level=3 odps.instance.remain.days=30 ";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(4, map.size());
    Assert.assertEquals("true", map.get("odps.table.drop.ignorenonexistent"));
    Assert.assertEquals("false", map.get("odps.instance.priority.autoadjust"));
    Assert.assertEquals("3", map.get("odps.instance.priority.level"));
    Assert.assertEquals("30", map.get("odps.instance.remain.days"));
  }

  @Test
  public void testParsePropertiesComplex() {
    String setProjectCommand = "setproject odps.security.ip.whitelist=192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(1, map.size());
    Assert.assertEquals("192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1,192.168.0.1", map.get("odps.security.ip.whitelist"));
  }

  @Test
  public void testParsePropertiesComplex2() {
    String setProjectCommand = "setproject odps.security.ip.whitelist=192.168.0.0,192.168.0.10 odps.security.vpc.whitelist=<vpc实例id1>[192.168.0.10,192.168.0.20],<vpc实例id2>";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(2, map.size());
    Assert.assertEquals("192.168.0.0,192.168.0.10", map.get("odps.security.ip.whitelist"));
    Assert.assertEquals("<vpc实例id1>[192.168.0.10,192.168.0.20],<vpc实例id2>", map.get("odps.security.vpc.whitelist"));
  }

  @Test
  public void testParsePropertiesComplex3() {
    String setProjectCommand = "setproject odps.security.ip.whitelist=192.168.0.0 odps.security.vpc.whitelist=\n";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();

    Assert.assertEquals(2, map.size());
    Assert.assertEquals("192.168.0.0", map.get("odps.security.ip.whitelist"));
    Assert.assertEquals("", map.get("odps.security.vpc.whitelist"));
  }

  @Test
  public void testParsePropertiesJson() {
    String setProjectCommand = "setproject odps.lifecycle.config={\"TierToLowFrequency\": {\"DaysAfterLastModificationGreaterThan\": \"7\", \"NumberOfVersionsLessThan\": \"5\"}} happy=true";
    SetProjectCommand command = SetProjectCommand.parse(setProjectCommand, null);
    Assert.assertNotNull(command);
    Map<String, String> map = command.parseProperties();
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("{\"TierToLowFrequency\": {\"DaysAfterLastModificationGreaterThan\": \"7\", \"NumberOfVersionsLessThan\": \"5\"}}", map.get("odps.lifecycle.config"));
  }
}
