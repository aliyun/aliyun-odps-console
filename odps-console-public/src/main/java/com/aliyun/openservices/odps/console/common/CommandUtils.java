package com.aliyun.openservices.odps.console.common;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.aliyun.odps.utils.StringUtils;

public class CommandUtils {

  public static String longToDateTime(String time) {
    if (StringUtils.isNullOrEmpty(time)) {
      return null;
    }

    DateTimeFormatter
        dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(
            ZoneId.systemDefault());
    return dateTimeFormatter.format(Instant.ofEpochSecond(Long.parseLong(time)));
  }
}
