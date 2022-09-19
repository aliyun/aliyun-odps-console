package com.aliyun.openservices.odps.console.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

public class FormatUtils {
  public static final DateFormat DATETIME_FORMAT;
  public static final Gson GSON;
  public static final Calendar ISO8601_LOCAL_CALENDAR =  new Calendar.Builder()
      .setCalendarType("iso8601")
      .setLenient(true)
      .build();

  private static final String ZERO_NANO = "000000000";

  static {
    DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    DATETIME_FORMAT.setCalendar(ISO8601_LOCAL_CALENDAR);

    JsonSerializer<Date> dateTimeSerializer = (date, type, jsonSerializationContext) -> {
      if (date == null) {
        return null;
      }
      return new JsonPrimitive(DATETIME_FORMAT.format(date));
    };
    JsonSerializer<Timestamp> timestampSerializer = (timestamp, type, jsonSerializationContext) -> {
      if (timestamp == null) {
        return null;
      }

      return new JsonPrimitive(formatTimestamp(timestamp, DATETIME_FORMAT));
    };
    JsonSerializer<SimpleStruct> structSerializer = (struct, type, jsonSerializationContext) -> {
      if (struct == null) {
        return null;
      }
      return FormatUtils.normalizeStruct(struct);
    };
    GSON = new GsonBuilder()
        .registerTypeAdapter(Date.class, dateTimeSerializer)
        .registerTypeAdapter(Timestamp.class, timestampSerializer)
        .registerTypeAdapter(SimpleStruct.class, structSerializer)
        .serializeNulls()
        .create();
  }

  public static class FormattedResultSet implements Iterator<String> {
    private ResultSet resultSet;
    private Gson gson;
    private DateFormat datetimeFormat;
    private Map<String, Integer> width;
    private String frame;
    private String title;

    private long numLinesReturned = 0;

    public FormattedResultSet(
        ResultSet resultSet,
        Gson gson,
        DateFormat datetimeFormat) {
      this.resultSet = resultSet;
      this.gson = gson;
      this.datetimeFormat = datetimeFormat;
      this.width = ODPSConsoleUtils.getDisplayWidth(resultSet.getTableSchema().getColumns(),
                                                    null,
                                                    null);
      this.frame = ODPSConsoleUtils.makeOutputFrame(width);
      this.title = ODPSConsoleUtils.makeTitle(resultSet.getTableSchema().getColumns(), width);
    }

    @Override
    public boolean hasNext() {
      // Total number of lines to return is resultSet.getRecordCount() + 4, since there are title
      // and frames.
      return resultSet.hasNext() || numLinesReturned < resultSet.getRecordCount() + 4;
    }

    @Override
    public String next() {
      String line;
      if (numLinesReturned == 0
          || numLinesReturned == 2
          || numLinesReturned == resultSet.getRecordCount() + 3) {
        // The first, the third and the last line
        line = frame;
      } else if (numLinesReturned == 1) {
        // The second line
        line = title;
      } else if (numLinesReturned < resultSet.getRecordCount() + 3) {
        line = formatRecord(resultSet.next(), width, gson, datetimeFormat);
      } else {
        throw new NoSuchElementException("No more elements");
      }

      numLinesReturned += 1;
      return line;
    }

    public long getRecordCount() {
      return resultSet.getRecordCount();
    }
  }

  public static List<String> formatResultSet(
      ResultSet resultSet,
      Gson gson,
      DateFormat datetimeFormat,
      DateFormat timestampFormat) {
    List<String> formattedRecords = new ArrayList<>();
    Map<String, Integer> width = ODPSConsoleUtils.getDisplayWidth(
        resultSet.getTableSchema().getColumns(), null, null);
    String frame = ODPSConsoleUtils.makeOutputFrame(width);
    String title = ODPSConsoleUtils.makeTitle(resultSet.getTableSchema().getColumns(), width);

    formattedRecords.add(frame);
    formattedRecords.add(title);
    formattedRecords.add(frame);

    while (resultSet.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Record record = resultSet.next();
      formattedRecords.add(formatRecord(record, width, gson, datetimeFormat));
    }

    formattedRecords.add(frame);
    return formattedRecords;
  }

  public static String formatRecord(
      Record record,
      Map<String, Integer> width,
      Gson gson,
      DateFormat datetimeFormat) {
    StringBuilder sb = new StringBuilder();
    sb.append("| ");

    for (int i = 0; i < record.getColumnCount(); i++) {
      Object o = record.get(i);
      if (OdpsType.DATE.equals(record.getColumns()[i].getTypeInfo().getOdpsType())) {
        o = ((ArrayRecord) record).getDate(i, ISO8601_LOCAL_CALENDAR);
      }

      String res = formatField(
          o,
          record.getColumns()[i].getTypeInfo(),
          gson,
          datetimeFormat);
      sb.append(res);
      if (res.length() < width.get(record.getColumns()[i].getName())) {
        int extraLen = width.get(record.getColumns()[i].getName()) - res.length();
        while (extraLen-- > 0) {
          sb.append(" ");
        }
      }

      sb.append(" | ");
    }

    return sb.toString();
  }

  public static String formatField(Object object, TypeInfo typeInfo) {
    return formatField(object, typeInfo, GSON, DATETIME_FORMAT);
  }

  public static String formatTimestamp(java.sql.Timestamp value, DateFormat datetimeFormat) {
    if (value.getNanos() == 0) {
      return datetimeFormat.format(value);
    } else {
      String nanosValueStr = Integer.toString(value.getNanos());
      nanosValueStr = ZERO_NANO.substring(0, (9 - nanosValueStr.length())) + nanosValueStr;

      // Truncate trailing zeros
      char[] nanosChar = new char[nanosValueStr.length()];
      nanosValueStr.getChars(0, nanosValueStr.length(), nanosChar, 0);
      int truncIndex = 8;
      while (nanosChar[truncIndex] == '0') {
        truncIndex--;
      }
      nanosValueStr = new String(nanosChar, 0, truncIndex + 1);

      return String.format("%s.%s", datetimeFormat.format(value), nanosValueStr);
    }
  }

  public static String formatField(
      Object object,
      TypeInfo typeInfo,
      Gson gson,
      DateFormat datetimeFormat) {
    if (object == null) {
      return "NULL";
    }

    switch (typeInfo.getOdpsType()) {
      case DATETIME: {
        return datetimeFormat.format((Date) object);
      }
      case TIMESTAMP: {
        return formatTimestamp((java.sql.Timestamp) object, datetimeFormat);
      }
      case ARRAY:
      case MAP: {
        return gson.toJson(object);
      }
      case STRUCT: {
        return formatStruct(object, gson);
      }
      case STRING: {
        if (object instanceof byte []) {
          return new String((byte []) object);
        }
        return object.toString();
      }
      default: {
        return object.toString();
      }
    }
  }

  public static String formatStruct(Object object, Gson gson) {
    return gson.toJson(normalizeStruct(object));
  }

  public static JsonElement normalizeStruct(Object object) {
    Map<String, Object> values = new LinkedHashMap<>();
    Struct struct = (Struct) object;
    for (int i = 0; i < struct.getFieldCount(); i++) {
      values.put(struct.getFieldName(i), struct.getFieldValue(i));
    }

    return new Gson().toJsonTree(values);
  }
}
