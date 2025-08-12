package com.aliyun.openservices.odps.console.xflow;

import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;


public class LineageHelper {

    public static final String LINEAGE_FORMAT_HINT = "Invalid lineage json format, Please refer to the example format:\n" +
            "{\n" +
            "    \"SrcEntities\":\n" +
            "    [\n" +
            "        {\n" +
            "            \"QualifiedName\": \"maxcompute-table.project1.table1\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"ResourceType\": \"dataset\",\n" +
            "                \"ResourceUse\": \"train\"\n" +
            "            }\n" +
            "        },\n" +
            "        {\n" +
            "            \"QualifiedName\": \"maxcompute-offlinemodel.project1.model1\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"EntityType\": \"oss-file\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"Bucket\": \"llm_bucket\",\n" +
            "                \"Path\": \"/models/\",\n" +
            "                \"ResourceType\": \"model\",\n" +
            "                \"ResourceUse\": \"base\"\n" +
            "            }\n" +
            "        },\n" +
            "        {\n" +
            "            \"EntityType\": \"nas-file\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"Uri\": \"nas://0bd314b6ac.cn-hangzhou/\",\n" +
            "                \"DataSourceType\": \"NAS\"\n" +
            "            }\n" +
            "        }\n" +
            "    ],\n" +
            "    \"DestEntities\":\n" +
            "    [\n" +
            "        {\n" +
            "            \"QualifiedName\": \"pai-dataset.d-k0s7kplxcs94oohsgt\",\n" +
            "            \"Name\": \"dataset-test1\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"ResourceUse\": \"train\",\n" +
            "                \"WorkspaceId\": \"54543\"\n" +
            "            }\n" +
            "        },\n" +
            "        {\n" +
            "            \"QualifiedName\": \"pai-model.model-fkj10rrenfasrd3kqh/1.0.0\",\n" +
            "            \"Name\": \"model-test1\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"ResourceUse\": \"base\",\n" +
            "                \"WorkspaceId\": \"54543\"\n" +
            "            }\n" +
            "        },\n" +
            "        {\n" +
            "            \"QualifiedName\": \"pai-eas.easynlp_pai_bert_tiny_zh_s2p1of14\",\n" +
            "            \"Name\": \"easynlp_pai_bert_tiny_zh_s2p1of14\",\n" +
            "            \"Attributes\":\n" +
            "            {\n" +
            "                \"WorkspaceId\": \"54543\"\n" +
            "            }\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    @interface JsonRequired {
    }

    private static class AnnotatedDeserializer<T> implements JsonDeserializer<T> {

        public T deserialize(JsonElement je, Type type, JsonDeserializationContext jdc) throws JsonParseException {
            T pojo = new Gson().fromJson(je, type);

            Field[] fields = pojo.getClass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName().equals("Attributes")) {
                    f.setAccessible(true);
                    Field[] attributesFields = f.getType().getDeclaredFields();
                    for (Field ff : attributesFields) {
                        if (ff.getAnnotation(JsonRequired.class) != null) {
                            try {
                                ff.setAccessible(true);
                                Object o = f.get(pojo);
                                if (o == null || ff.get(o) == null) {
                                    throw new JsonParseException("Missing required field in JSON: " + pojo.getClass().getName() + ".Attributes." + ff.getName());
                                }
                            } catch (IllegalArgumentException | IllegalAccessException e) {
                                throw new JsonParseException("Parse json failed: " + e.getMessage());
                            }
                        }
                    }
                }
                if (f.getAnnotation(JsonRequired.class) != null) {
                    try {
                        f.setAccessible(true);
                        if (f.get(pojo) == null) {
                            throw new JsonParseException("Missing required field in JSON: " + pojo.getClass().getName() + "." + f.getName());
                        }
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        throw new JsonParseException("Parse json failed: " + e.getMessage());
                    }
                }
            }
            return pojo;
        }
    }

    private static class Lineage {
        @JsonRequired
        public List<Object> SrcEntities;
        @JsonRequired
        public List<Object> DestEntities;
    }

    private static class MaxComputeTable {
        private static final String NAME = "maxcompute-table";


        private static class Attributes {
            private String ResourceType;
            private String ResourceUse;
        }

        @JsonRequired
        private String QualifiedName;
        private MaxComputeTable.Attributes Attributes;
    }

    private static class MaxComputeOfflineModel {
        private static final String NAME = "maxcompute-offlinemodel";

        @JsonRequired
        public String QualifiedName;
    }

    private static class OssFile {
        private static final String NAME = "oss-file";

        private static class Attributes {
            @JsonRequired
            private String Bucket;
            @JsonRequired
            private String Path;
            private String ResourceType;
            private String ResourceUse;
        }

        @JsonRequired
        private String EntityType;

        private OssFile.Attributes Attributes;
    }

    private static class NasFile {
        private static final String NAME = "nas-file";

        private static class Attributes {
            @JsonRequired
            private String Uri;
            private String DataSourceType;
        }

        @JsonRequired
        private String EntityType;
        private NasFile.Attributes Attributes;
    }

    private static class PaiDataset {
        private static final String NAME = "pai-dataset";

        private static class Attributes {
            private String ResourceUse;
            private String WorkspaceId;
            private String Provider;
        }

        @JsonRequired
        private String QualifiedName;
        private String Name;
        private PaiDataset.Attributes Attributes;
    }

    private static class PaiModel {
        private static final String NAME = "pai-model";

        private static class Attributes {
            private String ResourceUse;
            private String WorkspaceId;
            private String Provider;
        }

        @JsonRequired
        private String QualifiedName;
        private String Name;
        private PaiModel.Attributes Attributes;
    }

    private static class PaiEas {
        private static final String NAME = "pai-eas";

        private static class Attributes {
            private String WorkspaceId;
        }

        @JsonRequired
        private String QualifiedName;
        private String Name;
        private PaiEas.Attributes Attributes;
    }

    private static class MysqlTable {
        private static final String NAME = "mysql-table";

        @JsonRequired
        private String QualifiedName;
        private String Name;
    }

    private static class EmrTable {
        private static final String NAME = "emr-table";

        @JsonRequired
        private String QualifiedName;
        private String Name;
    }

    private static class HoloTable {
        private static final String NAME = "holodb-table";

        @JsonRequired
        private String QualifiedName;
        private String Name;
    }

    private static class CustomEntity {
        private static final String NAME_PREFIX = "custom-";

        @JsonRequired
        private String QualifiedName;
        private String Name;
    }

    public static boolean isValidLineageJson(String json) {
        Gson gson = new GsonBuilder().registerTypeAdapter(Lineage.class, new AnnotatedDeserializer()).create();
        Lineage lineageObj;
        try {
            lineageObj = gson.fromJson(json, Lineage.class);
            if (lineageObj == null ||
                    lineageObj.SrcEntities == null || lineageObj.SrcEntities.isEmpty() ||
                    lineageObj.DestEntities == null || lineageObj.DestEntities.isEmpty()) {
                return false;
            }
        } catch (JsonParseException e) {
            System.err.println(e.getMessage());
            return false;
        }

        boolean isValid = true;
        for (Object entity : lineageObj.SrcEntities) {
            Class<?> entityClass = getEntityClass((LinkedTreeMap<?, ?>) entity);
            if (entityClass == null) {
                return false;
            }
            boolean entityValid = false;
            try {
                new GsonBuilder().registerTypeAdapter(entityClass, new AnnotatedDeserializer()).create().fromJson(gson.toJson(entity), entityClass);
                entityValid = true;
            } catch (JsonParseException e) {
                System.err.println(e.getMessage());
            }
            isValid &= entityValid;
        }
        for (Object entity : lineageObj.DestEntities) {
            Class<?> entityClass = getEntityClass((LinkedTreeMap<?, ?>) entity);
            if (entityClass == null) {
                return false;
            }
            boolean entityValid = false;
            try {
                new GsonBuilder().registerTypeAdapter(entityClass, new AnnotatedDeserializer()).create().fromJson(gson.toJson(entity), entityClass);
                entityValid = true;
            } catch (JsonParseException e) {
                System.err.println(e.getMessage());
            }
            isValid &= entityValid;
        }
        return isValid;
    }

    private static Class<?> getEntityClass(LinkedTreeMap<?, ?> entity) {
        String entityType = (String) entity.get("EntityType");
        if (entityType == null) {
            String QualifiedName = (String) entity.get("QualifiedName");
            if (QualifiedName != null) {
                int index = QualifiedName.indexOf(".");
                if (index != -1) {
                    entityType = QualifiedName.substring(0, index);
                }
            }
        }
        if (entityType == null) {
            return null;
        }
        switch (entityType) {
            case MaxComputeTable.NAME:
                return MaxComputeTable.class;
            case MaxComputeOfflineModel.NAME:
                return MaxComputeOfflineModel.class;
            case OssFile.NAME:
                return OssFile.class;
            case NasFile.NAME:
                return NasFile.class;
            case PaiDataset.NAME:
                return PaiDataset.class;
            case PaiModel.NAME:
                return PaiModel.class;
            case PaiEas.NAME:
                return PaiEas.class;
            case MysqlTable.NAME:
                return MysqlTable.class;
            case EmrTable.NAME:
                return EmrTable.class;
            case HoloTable.NAME:
                return HoloTable.class;
            default:
                if (entityType.startsWith(CustomEntity.NAME_PREFIX)) {
                    return CustomEntity.class;
                } else {
                    System.err.println("Unsupport entity type: " + entityType);
                    return null;
                }
        }
    }

}
