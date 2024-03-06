package com.aliyun.openservices.odps.console.common;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

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
  public static String getRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
