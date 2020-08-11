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

package com.aliyun.openservices.odps.console;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TimeZone;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Created by dongxiao on 2019/8/19.
 */
public abstract class RecordPrinter {
  protected ExecutionContext context;
  private DateFormat dateFormat = null;
  private DateFormat timestampFormat = null;

  // Gson customized serializer
  private JsonSerializer<Date> dateTimeSerializer;
  private JsonSerializer<Timestamp> timestampSerializer;
  private JsonSerializer<SimpleStruct> structSerializer;
  private Gson gson;

  protected RecordPrinter(TableSchema schema, ExecutionContext context) {
    this.context = context;

    initDateFormat();
    initJsonConfig();
  }

  public static RecordPrinter createReporter(TableSchema schema, ExecutionContext context) {
    if (context.isMachineReadable()) {
      return new CSVRecordPrinter(schema, context);
    } else {
      return new HumanReadableRecordPrinter(schema, context);
    }
  }

  public JsonElement normalizeStruct(Object object) {
    LinkedHashMap<String, Object> values = new LinkedHashMap<String, Object>();
    Struct struct = (Struct) object;
    for (int i = 0; i < struct.getFieldCount(); i++) {
      values.put(struct.getFieldName(i), struct.getFieldValue(i));
    }

    return new Gson().toJsonTree(values);
  }

  public String formatStruct(Object object) {
    return gson.toJson(normalizeStruct(object));
  }

  public String formatField(Object object, TypeInfo typeInfo) {
    if (object == null) {
      return "NULL";
    }

    switch (typeInfo.getOdpsType()) {
      case DATETIME: {
        return dateFormat.format((Date) object);
      }
      case TIMESTAMP: {
        return timestampFormat.format((java.sql.Timestamp) object);
      }
      case ARRAY:
      case MAP: {
        return gson.toJson(object);
      }
      case STRUCT: {
        return formatStruct(object);
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

  public void initJsonConfig() {
    dateTimeSerializer = new JsonSerializer<Date>() {
      @Override
      public JsonElement serialize(Date date, Type type, JsonSerializationContext jsonSerializationContext) {
        if (date == null) {
          return null;
        }
        return new JsonPrimitive(dateFormat.format(date));
      }
    };

    timestampSerializer = new JsonSerializer<Timestamp>() {
      @Override
      public JsonElement serialize(Timestamp timestamp, Type type, JsonSerializationContext jsonSerializationContext) {
        if (timestamp == null) {
          return null;
        }
        return new JsonPrimitive(timestampFormat.format(timestamp));
      }
    };

    structSerializer = new JsonSerializer<SimpleStruct>() {
      @Override
      public JsonElement serialize(SimpleStruct struct, Type type, JsonSerializationContext jsonSerializationContext) {
        if (struct == null) {
          return null;
        }
        return normalizeStruct(struct);
      }
    };

    gson = new GsonBuilder()
        .registerTypeAdapter(Date.class, dateTimeSerializer)
        .registerTypeAdapter(Timestamp.class, timestampSerializer)
        .registerTypeAdapter(SimpleStruct.class, structSerializer)
        .serializeNulls()
        .create();
  }

  public void initDateFormat() {
    dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    try {
      String timezone = context.getSqlTimezone();
      if (timezone != null) {
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        timestampFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        context.getOutputWriter().writeDebug("SQL records formatted in timezone: " + timezone);
      }
    } catch (Exception e) {
      // ignore
    }
  }

  public abstract void printFrame();

  public abstract void printTitle() throws ODPSConsoleException;

  public abstract void printRecord(Record record) throws ODPSConsoleException;

}
