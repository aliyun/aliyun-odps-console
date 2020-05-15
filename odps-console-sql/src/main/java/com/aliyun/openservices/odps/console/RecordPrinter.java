package com.aliyun.openservices.odps.console;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by dongxiao on 2019/8/19.
 */
public class RecordPrinter {
    private ExecutionContext context;
    private Odps odps;
    private DateFormat dateFormat = null;
    private DateFormat timestampFormat = null;

    // Gson customized serializer
    private JsonSerializer<Date> dateTimeSerializer;
    private JsonSerializer<Timestamp> timestampSerializer;
    private JsonSerializer<SimpleStruct> structSerializer;
    private Gson gson;

    Map<String, Integer> width = null;
    String frame;
    String title;

    public RecordPrinter(TableSchema schema, Odps odps, ExecutionContext context) {
        this.context = context;
        this.odps = odps;

        initDateFormat();
        initJsonConfig();

        width =
                ODPSConsoleUtils.getDisplayWidth(schema.getColumns(), null, null);
        frame = ODPSConsoleUtils.makeOutputFrame(width);
        title = ODPSConsoleUtils.makeTitle(schema.getColumns(), width);
    }

    public String formatRecord(Record record, Map<String, Integer> width) {
        StringBuilder sb = new StringBuilder();
        sb.append("| ");

        for (int i = 0; i < record.getColumnCount(); i++) {
            String res = formatField(record.get(i), record.getColumns()[i].getTypeInfo());

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

            if (timezone == null) {
                timezone =
                        odps.projects().get(context.getProjectName()).getProperty("odps.sql.timezone");
            }
            if (timezone != null) {
                dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
                timestampFormat.setTimeZone(TimeZone.getTimeZone(timezone));
                context.getOutputWriter().writeDebug("SQL records formatted in timezone: " + timezone);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public void printFrame() {
        context.getOutputWriter().writeResult(frame);
    }

    public void printTitle() {
        context.getOutputWriter().writeResult(title);
    }

    public void printRecord(Record record) {
        context.getOutputWriter().writeResult(formatRecord(record, width));
    }
}
