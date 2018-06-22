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

package com.aliyun.odps.ship.upload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.nio.charset.Charset;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;

/**
 * 测试文件的按行读取
 * */
public class BlockRecordReaderTest {

  static{
    Constants.DEFAULT_IO_BUFFER_SIZE = 1 * 1024 *1024;
  }
  static String originalCharset;

  @BeforeClass
  public static void setToUTF8() throws Exception {
    originalCharset = System.getProperty("file.encoding");
    setSystemDefaultCharset("UTF-8");
  }

  @AfterClass
  public static void revertSystemCharset() throws Exception {
    setSystemDefaultCharset(originalCharset);
  }

  public static void setSystemDefaultCharset(String s)
      throws NoSuchFieldException, IllegalAccessException {
    System.setProperty("file.encoding", s);
    Field charset = Charset.class.getDeclaredField("defaultCharset");
    charset.setAccessible(true);
    charset.set(null, null);
  }
  
  /**
   * 测试文件,读取一行，行分隔符和列分隔符为一个字符
   * */
  @Test
  public void testReadLineOneline() throws Exception {

    /**
     * file content 123,abc 234,bbb
     * */
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_fd_end.txt"), 0L, 45L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);
    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    
    assertEquals("split length not equal", 10, firstLine.length);

    // 123||ab测试c,,,234||bb你好b,,,333||ccc,,,
    assertEquals("not equal", "123||ab测试c", new String(firstLine[0], "UTF-8"));
    assertEquals("not equal", "", new String(firstLine[1]));
    assertEquals("not equal", "", new String(firstLine[2]));
    assertEquals("not equal", "333||ccc", new String(firstLine[6]));
    assertEquals("not equal", "", new String(firstLine[9]));

  }

  /**
   * 测试读文件，读取多行, 行分隔符和列分隔符为一个字符
   * */
  @Test
  public void testReadLineMoreLine() throws Exception {
    /**
     * file content 123,abc 234,bbb
     * */

    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 15L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0]));
    assertEquals("not equal", "abc", new String(firstLine[1]));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    
    assertEquals("not equal", "234", new String(nextLine[0]));
    assertEquals("not equal", "bbb", new String(nextLine[1]));
  }

  /**
   * 测试读文件，单个字符分隔符，文件内容包括中文，编码格式为utf8
   * */
  @Test
  public void testReadLineChinese() throws Exception {

    /**
     * file content 123,ab测试c 234,bb你好b
     * */
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split_chinese.txt"), 0L, 27L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "ab测试c", new String(firstLine[1], "utf8"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bb你好b", new String(nextLine[1], "utf8"));

  }


  /**
   * 测试读文件，行分隔符和列分隔符为多个字符
   * */
  @Test
  public void testReadLine_more_char_split() throws Exception {

    // file content :
    // 123||abc,,,234||bbb,,,333||ccc,,,

    BlockInfo blockInfo = new BlockInfo(1L,  new File("src/test/resources/file/reader/more_char_split.txt"), 0L, 33L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0]));
    assertEquals("not equal", "abc", new String(firstLine[1]));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0]));
    assertEquals("not equal", "bbb", new String(nextLine[1]));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "333", new String(tLine[0]));
    assertEquals("not equal", "ccc", new String(tLine[1]));
  }

  /**
   * 测试读文件，行分隔符和列分隔符为多个字符，内容包含中文
   * */
  @Test
  public void testReadLineMoreCharChinese() throws Exception {

    // file content
    // 123||ab测试c,,,234||bb你好b,,,33啊啊3||ccc,,,
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese.txt"), 0L, 45L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "ab测试c", new String(firstLine[1], "utf8"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bb你好b", new String(nextLine[1], "utf8"));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "333", new String(tLine[0], "utf8"));
    assertEquals("not equal", "ccc", new String(tLine[1], "utf8"));

  }

  /**
   * 测试行分隔符和列分隔符为中文字符
   * */
  @Test
  public void testReadLineChineseSplit() throws Exception {
    // file content
    // 123列分隔符ab测试c行分隔符234列分隔符bb你好b行分隔符333列分隔符ccc行分隔符
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_chinese_spliter.txt"), 0L, 102L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "列分隔符", "行分隔符", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "ab测试c", new String(firstLine[1], "utf8"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bb你好b", new String(nextLine[1], "utf8"));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "333", new String(tLine[0], "utf8"));
    assertEquals("not equal", "ccc", new String(tLine[1], "utf8"));
  }

  /**
   * 测试文件内容包含中文，文件带有bom头
   * */
  @Test
  public void testReadLineChineseBom() throws Exception {

    // file content
    // 123||ab测试c,,,234||bb你好b,,,33啊啊3||ccc,,,

    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 0L, 54L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo,  "||", ",,,", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "ab测试c", new String(firstLine[1], "utf8"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bb你好b", new String(nextLine[1], "utf8"));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "33啊啊3", new String(tLine[0], "utf8"));
    assertEquals("not equal", "ccc", new String(tLine[1], "utf8"));

  }

  /**
   * 测试单个字符A行分隔符，目的是，A字符和正则表达式无关
   * */
  @Test
  public void testFieldDelimiterA() throws Exception {

    /**
     * file content 123||a测试bc||||234||bbb||||333||ccc|
     * */

    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split_A.txt"), 0L, 15L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "A", "\n", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "abc", new String(firstLine[1], "utf8"));
    
    line = reader.readLine();
    byte[][] nextLine = reader.splitLine(line);
    
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bbb", new String(nextLine[1], "utf8"));

    reader = new BlockRecordReader(blockInfo, "\u0041", "\n", false);
    
    line = reader.readLine();
    firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "abc", new String(firstLine[1], "utf8"));

    line = reader.readLine();
    nextLine = reader.splitLine(line);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bbb", new String(nextLine[1], "utf8"));
  }

  /**
   * 测试字符串split，包括行分隔符为正则表达式的特殊字符
   * */
  @Test
  public void testSpliter() throws Exception {

    /**
     * file content 123||a测试bc||||234||bbb||||333||ccc|
     * */

    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split_A.txt"), 0L, 15L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "[", "\n", false);


    byte[][] l = reader.splitLine("123[abc[456".getBytes());
    assertEquals("size not equal", 3, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[1], "utf8"));
    assertEquals("456 not equal", "456", new String(l[2], "utf8"));
    l = reader.splitLine("123[[abc[456[".getBytes());
    assertEquals("size not equal", 5, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("null not equal", "", new String(l[1], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[2], "utf8"));
    assertEquals("456 not equal", "456", new String(l[3], "utf8"));
    assertEquals("null not equal", "", new String(l[4], "utf8"));
    l = reader.splitLine("123[[abc[456[[[[".getBytes());
    assertEquals("size not equal", 8, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("null not equal", "", new String(l[1], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[2], "utf8"));
    assertEquals("456 not equal", "456", new String(l[3], "utf8"));
    assertEquals("null not equal", "", new String(l[4], "utf8"));
    assertEquals("null not equal", "", new String(l[5], "utf8"));
    assertEquals("null not equal", "", new String(l[6], "utf8"));
    assertEquals("null not equal", "", new String(l[7], "utf8"));

    reader = new BlockRecordReader(blockInfo,  ",", "\n", false);
    l = reader.splitLine("123,abc,456".getBytes());
    assertEquals("size not equal", 3, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[1], "utf8"));
    assertEquals("456 not equal", "456", new String(l[2], "utf8"));
    l = reader.splitLine("123,,abc,456,".getBytes());
    assertEquals("size not equal", 5, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("null not equal", "", new String(l[1], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[2], "utf8"));
    assertEquals("456 not equal", "456", new String(l[3], "utf8"));
    assertEquals("null not equal", "", new String(l[4], "utf8"));

    reader = new BlockRecordReader(blockInfo, ",", "\n", false);
    l = reader.splitLine("123,abc,456".getBytes());
    assertEquals("size not equal", 3, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[1], "utf8"));
    assertEquals("456 not equal", "456", new String(l[2], "utf8"));
    l = reader.splitLine("123,,abc,456,".getBytes());
    assertEquals("size not equal", 5, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("null not equal", "", new String(l[1], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[2], "utf8"));
    assertEquals("456 not equal", "456", new String(l[3], "utf8"));
    assertEquals("null not equal", "", new String(l[4], "utf8"));

    reader = new BlockRecordReader(blockInfo, "99", "\n", false);
    l = reader.splitLine("12399abc99456".getBytes());
    assertEquals("size not equal", 3, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[1], "utf8"));
    assertEquals("456 not equal", "456", new String(l[2], "utf8"));
    l = reader.splitLine("1239999abc9945699".getBytes());
    assertEquals("size not equal", 5, l.length);
    assertEquals("123 not equal", "123", new String(l[0], "utf8"));
    assertEquals("null not equal", "", new String(l[1], "utf8"));
    assertEquals("abc not equal", "abc", new String(l[2], "utf8"));
    assertEquals("456 not equal", "456", new String(l[3], "utf8"));
    assertEquals("null not equal", "", new String(l[4], "utf8"));

  }

  /**
   * 测试skip到文件的位置，续传读取下一行. 包括bom文件和非bom文件
   * */
  @Test
  public void testSkipAndRead() throws Exception {
    //pos is count by bytes
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 34L, 20L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);
    
    byte[] line = reader.readLine();
    byte[][] tLine = reader.splitLine(line);
    assertEquals("not equal", "33啊啊3", new String(tLine[0], "utf8"));
    assertEquals("not equal", "ccc", new String(tLine[1], "utf8"));

    BufferedReader inr2 =
        new BufferedReader(new FileReader(
            "src/test/resources/file/reader/more_char_split_chinese.txt"),  1 * 1024 * 1024);
    inr2.close();
    BlockInfo blockInfo2 = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese.txt"), 34L, 20L);
    BlockRecordReader reader2 = new BlockRecordReader(blockInfo2, "||", ",,,", false);
    
    line = reader2.readLine();
    assertEquals("not null", line, null);
  }
  
  /**
   * 测试读文件，多个字符分隔，内容包含中文，编码格式为gbk
   * */
  @Test
  public void testReadLineMoreCharChineseWithGBK() throws Exception {

    // file content
    // 123||ab测试c,,,234||bb你好b,,,33啊啊3||ccc,,,
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_gbk.txt"), 0L, 43L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "gbk"));
    assertEquals("not equal", "ab测试c", new String(firstLine[1], "gbk"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "gbk"));
    assertEquals("not equal", "bb你好b", new String(nextLine[1], "gbk"));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "333", new String(tLine[0], "gbk"));
    assertEquals("not equal", "ccc", new String(tLine[1], "gbk"));

  }
  
  /**
   * 测试行分隔符包含列分隔符，正常执行
   * */
  @Test
  public void testRecordDelimiterIncludeFieldDelimiter() throws Exception {

    /**
     * file content 123||a测试bc||||234||bbb||||333||ccc|
     * */
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_mix.txt"), 0L, 39L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", "||||", false);

    byte[] line = reader.readLine();
    byte[][] firstLine = reader.splitLine(line);
    assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
    assertEquals("not equal", "a测试bc", new String(firstLine[1], "utf8"));

    byte[] line2 = reader.readLine();
    byte[][] nextLine = reader.splitLine(line2);
    assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
    assertEquals("not equal", "bbb", new String(nextLine[1], "utf8"));

    byte[] line3 = reader.readLine();
    byte[][] tLine = reader.splitLine(line3);
    assertEquals("not equal", "333", new String(tLine[0], "utf8"));
    assertEquals("not equal", "ccc|", new String(tLine[1], "utf8"));

  }

  /**
   * 测试文件被分成多个block读
   **/
  @Test
  public void testReadMultiBlocksNormalCase() throws Exception {
    // block split exactly at line split
    {
      //first block will read 2 line
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 8L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      byte[][] secondLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(secondLine[0]));
      assertEquals("not equal", "bbb", new String(secondLine[1]));

      //second block read nothing
      BlockInfo blockInfo2 = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 8L, 7L);
      reader = new BlockRecordReader(blockInfo2, ",", "\n", false);
      secondLine = reader.readTextRecord();
      assertNull("not null", secondLine);
    }

    // block split after line split
    {
      //first block will read 2 line
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 9L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      byte[][] secondLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(secondLine[0]));
      assertEquals("not equal", "bbb", new String(secondLine[1]));

      //second block read nothing
      BlockInfo blockInfo2 = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 9L, 6L);
      reader = new BlockRecordReader(blockInfo2, ",", "\n", false);
      secondLine = reader.readTextRecord();
      assertNull("not null", secondLine);
    }

    // block split before line split
    {
      //first block read first line
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 7L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      firstLine = reader.readTextRecord();
      assertNull("not null", firstLine);

      //second block read second line
      BlockInfo blockInfo2 = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 7L, 8L);
      reader = new BlockRecordReader(blockInfo2, ",", "\n", false);
      byte[][] secondLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(secondLine[0]));
      assertEquals("not equal", "bbb", new String(secondLine[1]));

      secondLine = reader.readTextRecord();
      assertNull("not null", secondLine);
    }
  }

  /**
   * 测试文件被分成多个block读, 
   **/
  @Test
  public void testReadMultiBlocksAbnormalCase() throws Exception {
    //test startPos is out of boundary,
    {
      //first block will read 2 line
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 100L, 8L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

      byte[][] secondLine = reader.readTextRecord();
      assertNull("not null", secondLine);
    }

    // test offset is out of boundary
    {
      //first block will read 2 line
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 100L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      byte[][] secondLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(secondLine[0]));
      assertEquals("not equal", "bbb", new String(secondLine[1]));

      //second block read nothing
      secondLine = reader.readTextRecord();
      assertNull("not null", secondLine);
    }
  }

  /**
   * 测试文件被分成多个Block读，且行分隔符为多个字符
   * */
  @Test
  public void testReadMultiBlocksWithMoreCharSplit() throws Exception {

    // file content :
    // 123||abc,,,234||bbb,,,333||ccc,,,
    // first block， read 2 record
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split.txt"), 0L, 16L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      byte[][] nextLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(nextLine[0]));
      assertEquals("not equal", "bbb", new String(nextLine[1]));

      byte[][] tLine = reader.readTextRecord();
      assertNull("not null", tLine);
    }

    // second block， read 1 record
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split.txt"), 16L, 16L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "333", new String(firstLine[0]));
      assertEquals("not equal", "ccc", new String(firstLine[1]));
    }

    /**
     * 测试block恰好切分在行分隔符之间
     * 第二行的分隔符的偏移是20-22，第一个block切分到21，恰好切分在第二个行分隔符之间。
     * 期望第一个block读前两行数据，第2个block从第三行数据开始读
     **/
    // first block， read 2 record
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split.txt"), 0L, 21L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0]));
      assertEquals("not equal", "abc", new String(firstLine[1]));

      byte[][] nextLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(nextLine[0]));
      assertEquals("not equal", "bbb", new String(nextLine[1]));

      byte[][] tLine = reader.readTextRecord();
      assertNull("not null", tLine);
    }

    // second block， read 1 record
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split.txt"), 21L, 16L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "333", new String(firstLine[0]));
      assertEquals("not equal", "ccc", new String(firstLine[1]));
    }

  }

  /**
   * 测试bom文件被分成多个block
   * */
  @Test
  public void testReadMultiBlocksWithBom() throws Exception {

    // file content
    // 123||ab测试c,,,234||bb你好b,,,33啊啊3||ccc,,,
    // first block read 2 lines
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 0L, 27L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123", new String(firstLine[0], "utf8"));
      assertEquals("not equal", "ab测试c", new String(firstLine[1], "utf8"));

      byte[][] nextLine = reader.readTextRecord();
      assertEquals("not equal", "234", new String(nextLine[0], "utf8"));
      assertEquals("not equal", "bb你好b", new String(nextLine[1], "utf8"));

      byte[][] tLine = reader.readTextRecord();
      assertNull("not null", tLine);
    }
    // second block read 1 line
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 27L, 54L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo,  "||", ",,,", false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "33啊啊3", new String(firstLine[0], "utf8"));
      assertEquals("not equal", "ccc", new String(firstLine[1], "utf8"));
    }
  }

  /**
   * 测试ignore header, 忽略第一行，从第二行开始读
   * */
  @Test
  public void testReadIgnoreHeader() throws Exception {
    /**
     * file content 123,abc 234,bbb, first line will ignore
     * */
    BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/one_char_split.txt"), 0L, 15L);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, ",", "\n", true);

    byte[][] firstLine = reader.readTextRecord();
    assertEquals("not equal", "234", new String(firstLine[0]));
    assertEquals("not equal", "bbb", new String(firstLine[1]));

    byte[][] secondLine = reader.readTextRecord();
    assertNull("not null", secondLine);
  }

  /**
   * 测试行分隔符与bom头冲突
   * */
  @Test
  public void testRDConflictWithBom() throws Exception {

    // file content
    // bom head is consumed by bom detected, only 1 line in block
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 0L, 54L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo, ",,,,", Character.toString((char)0xFF), false);

      byte[][] firstLine = reader.readTextRecord();
      assertEquals("not equal", "123||ab测试c,,,234||bb你好b,,,33啊啊3||ccc,,,", new String(firstLine[0], "utf8"));

      byte[][] tLine = reader.readTextRecord();
      assertNull("not null", tLine);
    }
    // ignore first line, no record read in this case
    {
      BlockInfo blockInfo = new BlockInfo(1L, new File("src/test/resources/file/reader/more_char_split_chinese_bom.txt"), 0L, 54L);
      BlockRecordReader reader = new BlockRecordReader(blockInfo,  "||", Character.toString((char)0xFF), true);

      byte[][] firstLine = reader.readTextRecord();
      assertNull("not null", firstLine);
    }
  }
}
