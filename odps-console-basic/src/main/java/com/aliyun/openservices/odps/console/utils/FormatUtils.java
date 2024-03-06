package com.aliyun.openservices.odps.console.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

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
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class FormatUtils {
  public static final Gson DEFAULT_COMPLEX_TYPE_FORMAT_GSON;
  public static final DateTimeFormatter DATETIME_FORMATTER;
  public static final DateTimeFormatter TIMESTAMP_FORMATTER;
  private static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

  static {
    DATETIME_FORMATTER = DateTimeFormatter.ofPattern(TIME_PATTERN)
            .withZone(ZoneId.systemDefault())
            .withResolverStyle(ResolverStyle.LENIENT);
    TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter()
            .withZone(ZoneId.systemDefault());

    JsonSerializer<LocalDate> dateSerializer = (date, type, jsonSerializationContext) -> {
      if (date == null) {
        return null;
      }
      return new JsonPrimitive(date.toString());
    };
    JsonSerializer<ZonedDateTime> dateTimeSerializer = (date, type, jsonSerializationContext) -> {
      if (date == null) {
        return null;
      }
      return new JsonPrimitive(DATETIME_FORMATTER.format(date));
    };
    JsonSerializer<Instant> timestampSerializer = (timestamp, type, jsonSerializationContext) -> {
      if (timestamp == null) {
        return null;
      }
      return new JsonPrimitive(TIMESTAMP_FORMATTER.format(timestamp));
    };
    JsonSerializer<SimpleStruct> structSerializer = (struct, type, jsonSerializationContext) -> {
      if (struct == null) {
        return null;
      }
      return FormatUtils.normalizeStruct(struct, jsonSerializationContext);
    };
    DEFAULT_COMPLEX_TYPE_FORMAT_GSON = new GsonBuilder()
        .registerTypeAdapter(LocalDate.class, dateSerializer)
        .registerTypeAdapter(ZonedDateTime.class, dateTimeSerializer)
        .registerTypeAdapter(Instant.class, timestampSerializer)
        .registerTypeAdapter(SimpleStruct.class, structSerializer)
        .serializeNulls()
        .create();
  }

  public static class FormattedResultSet implements Iterator<String> {
    private ResultSet resultSet;
    private Gson gson;
    private DateTimeFormatter datetimeFormat;
    private Map<String, Integer> width;
    private String frame;
    private String title;

    private long numLinesReturned = 0;

    public FormattedResultSet(ResultSet resultSet, Gson gson, DateTimeFormatter datetimeFormat) {
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

  public static String formatRecord(
      Record record,
      Map<String, Integer> width,
      Gson gson,
      DateTimeFormatter datetimeFormat) {
    StringBuilder sb = new StringBuilder();
    sb.append("| ");

    for (int i = 0; i < record.getColumnCount(); i++) {
      String res = formatField(record, i, record.getColumns()[i].getTypeInfo(), gson, datetimeFormat);

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

  public static String formatField(Record r, int idx, TypeInfo typeInfo) {
    return formatField(r, idx, typeInfo, DEFAULT_COMPLEX_TYPE_FORMAT_GSON, DATETIME_FORMATTER);
  }

  public static String formatField(Record r, int idx, TypeInfo typeInfo, Gson gson, DateTimeFormatter datetimeFormat) {
    if (r.get(idx) == null) {
      return "NULL";
    }

    switch (typeInfo.getOdpsType()) {
      case DATETIME: {
        ZonedDateTime zdt = ((ArrayRecord) r).getDatetimeAsZonedDateTime(idx);
        return datetimeFormat.format(zdt);
      }
      case TIMESTAMP: {
        Instant instant = ((ArrayRecord) r).getTimestampAsInstant(idx);
        return TIMESTAMP_FORMATTER.format(instant);
      }
      case TIMESTAMP_NTZ: {
        LocalDateTime localDateTime = ((ArrayRecord) r).getTimestampNtz(idx);
        return localDateTime.format(TIMESTAMP_FORMATTER);
      }
      case ARRAY:
      case MAP:
      case STRUCT: {
        return gson.toJson(r.get(idx));
      }
      case STRING: {
        // get(idx) might return byte[]
        return r.getString(idx);
      }
      default: {
        return r.get(idx).toString();
      }
    }
  }

  public static JsonElement normalizeStruct(Object object, JsonSerializationContext context) {
    Map<String, Object> values = new LinkedHashMap<>();
    Struct struct = (Struct) object;
    for (int i = 0; i < struct.getFieldCount(); i++) {
      values.put(struct.getFieldName(i), struct.getFieldValue(i));
    }

    return context.serialize(values);
  }
}
