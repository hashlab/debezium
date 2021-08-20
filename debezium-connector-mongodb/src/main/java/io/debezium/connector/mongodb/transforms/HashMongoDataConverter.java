/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.*;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState.ArrayEncoding;
import io.debezium.schema.FieldNameSelector.FieldNamer;

/**
 * MongoDataConverter handles translating MongoDB strings to Kafka Connect schemas and row data to Kafka
 * Connect records.
 *
 * @author Sairam Polavarapu
 */
public class HashMongoDataConverter extends MongoDataConverter {

    private final ArrayList<String> fieldsToFloat;

    public HashMongoDataConverter(ArrayEncoding arrayEncoding, FieldNamer<String> fieldNamer, String[] fieldsToFloat, boolean sanitizeValue) {
        super(arrayEncoding, fieldNamer, sanitizeValue);
        this.fieldsToFloat = new ArrayList<>(Arrays.asList(fieldsToFloat));
    }

    public void convertFieldValue(Entry<String, BsonValue> keyvalueforStruct, Struct struct, Schema schema) {
        Object colValue = null;
        Object originalValue;
        String key = fieldNamer.fieldNameFor(keyvalueforStruct.getKey());
        BsonType type = keyvalueforStruct.getValue().getBsonType();

        switch (type) {

            case NULL:
                colValue = null;
                break;

            case STRING:
                colValue = keyvalueforStruct.getValue().asString().getValue().toString();
                break;

            case OBJECT_ID:
                colValue = keyvalueforStruct.getValue().asObjectId().getValue().toString();
                break;

            case DOUBLE:
                colValue = keyvalueforStruct.getValue().asDouble().getValue();
                break;

            case BINARY:
                colValue = keyvalueforStruct.getValue().asBinary().getData();
                break;

            case INT32:
                originalValue = keyvalueforStruct.getValue().asInt32().getValue();
                colValue = convertValueToFloat(schema.name(), keyvalueforStruct.getKey(), originalValue);
                break;

            case INT64:
                originalValue = keyvalueforStruct.getValue().asInt64().getValue();
                colValue = convertValueToFloat(schema.name(), keyvalueforStruct.getKey(), originalValue);
                break;

            case BOOLEAN:
                colValue = keyvalueforStruct.getValue().asBoolean().getValue();
                break;

            case DATE_TIME:
                colValue = new Date(keyvalueforStruct.getValue().asDateTime().getValue());
                break;

            case JAVASCRIPT:
                colValue = keyvalueforStruct.getValue().asJavaScript().getCode();
                break;

            case JAVASCRIPT_WITH_SCOPE:
                Struct jsStruct = new Struct(schema.field(key).schema());
                Struct jsScopeStruct = new Struct(
                        schema.field(key).schema().field("scope").schema());
                jsStruct.put("code", keyvalueforStruct.getValue().asJavaScriptWithScope().getCode());
                BsonDocument jwsDoc = keyvalueforStruct.getValue().asJavaScriptWithScope().getScope().asDocument();

                for (Entry<String, BsonValue> jwsDocKey : jwsDoc.entrySet()) {
                    convertFieldValue(jwsDocKey, jsScopeStruct, schema.field(key).schema());
                }

                jsStruct.put("scope", jsScopeStruct);
                colValue = jsStruct;
                break;

            case REGULAR_EXPRESSION:
                Struct regexStruct = new Struct(schema.field(key).schema());
                regexStruct.put("regex", keyvalueforStruct.getValue().asRegularExpression().getPattern());
                regexStruct.put("options", keyvalueforStruct.getValue().asRegularExpression().getOptions());
                colValue = regexStruct;
                break;

            case TIMESTAMP:
                colValue = new Date(1000L * keyvalueforStruct.getValue().asTimestamp().getTime());
                break;

            case DECIMAL128:
                colValue = keyvalueforStruct.getValue().asDecimal128().getValue().toString();
                break;

            case DOCUMENT:
                Field field = schema.field(key);
                if (field == null) {
                    throw new DataException("Failed to find field '" + key + "' in schema " + schema.name());
                }
                Schema documentSchema = field.schema();
                Struct documentStruct = new Struct(documentSchema);
                BsonDocument docs = keyvalueforStruct.getValue().asDocument();

                for (Entry<String, BsonValue> doc : docs.entrySet()) {
                    convertFieldValue(doc, documentStruct, documentSchema);
                }

                colValue = documentStruct;
                break;

            case ARRAY:
                if (keyvalueforStruct.getValue().asArray().isEmpty()) {
                    // export empty arrays as null for Avro
                    if (sanitizeValue) {
                        return;
                    }

                    switch (arrayEncoding) {
                        case ARRAY:
                            colValue = new ArrayList<>();
                            break;
                        case DOCUMENT:
                            final Schema fieldSchema = schema.field(key).schema();
                            colValue = new Struct(fieldSchema);
                            break;
                    }
                }
                else {
                    switch (arrayEncoding) {
                        case ARRAY:
                            BsonType valueType = keyvalueforStruct.getValue().asArray().get(0).getBsonType();
                            List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();
                            ArrayList<Object> list = new ArrayList<>();

                            arrValues.stream().forEach(arrValue -> {
                                final Schema valueSchema;
                                if (Arrays.asList(BsonType.ARRAY, BsonType.DOCUMENT).contains(valueType)) {
                                    valueSchema = schema.field(key).schema().valueSchema();
                                }
                                else {
                                    valueSchema = null;
                                }
                                convertFieldValue(valueSchema, valueType, arrValue, list);
                            });
                            colValue = list;
                            break;
                        case DOCUMENT:
                            final BsonArray array = keyvalueforStruct.getValue().asArray();
                            final Map<String, BsonValue> convertedArray = new HashMap<>();
                            final Schema arraySchema = schema.field(key).schema();
                            final Struct arrayStruct = new Struct(arraySchema);
                            for (int i = 0; i < array.size(); i++) {
                                convertedArray.put(arrayElementStructName(i), array.get(i));
                            }
                            convertedArray.entrySet().forEach(x -> {
                                final Schema elementSchema = schema.field(key).schema();
                                convertFieldValue(x, arrayStruct, elementSchema);
                            });
                            colValue = arrayStruct;
                            break;
                    }
                }
                break;

            default:
                return;
        }
        struct.put(key, keyvalueforStruct.getValue().isNull() ? null : colValue);
    }

    public void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        String key = fieldNamer.fieldNameFor(keyValuesforSchema.getKey());
        BsonType type = keyValuesforSchema.getValue().getBsonType();
        Schema convertedSchema;

        switch (type) {

            case NULL:
            case STRING:
            case JAVASCRIPT:
            case OBJECT_ID:
            case DECIMAL128:
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
                break;

            case DOUBLE:
                builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
                break;

            case BINARY:
                builder.field(key, Schema.OPTIONAL_BYTES_SCHEMA);
                break;

            case INT32:
                convertedSchema = convertSchemaToFloat(builder.name(), key, Schema.OPTIONAL_INT32_SCHEMA);
                builder.field(key, convertedSchema);
                break;

            case INT64:
                convertedSchema = convertSchemaToFloat(builder.name(), key, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(key, convertedSchema);
                break;

            case DATE_TIME:
            case TIMESTAMP:
                builder.field(key, org.apache.kafka.connect.data.Timestamp.builder().optional().build());
                break;

            case BOOLEAN:
                builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                break;

            case JAVASCRIPT_WITH_SCOPE:
                SchemaBuilder jswithscope = SchemaBuilder.struct().name(builder.name() + "." + key);
                jswithscope.field("code", Schema.OPTIONAL_STRING_SCHEMA);
                SchemaBuilder scope = SchemaBuilder.struct().name(jswithscope.name() + ".scope").optional();
                BsonDocument jwsDocument = keyValuesforSchema.getValue().asJavaScriptWithScope().getScope().asDocument();

                for (Entry<String, BsonValue> jwsDocumentKey : jwsDocument.entrySet()) {
                    addFieldSchema(jwsDocumentKey, scope);
                }

                Schema scopeBuild = scope.build();
                jswithscope.field("scope", scopeBuild).build();
                builder.field(key, jswithscope);
                break;

            case REGULAR_EXPRESSION:
                SchemaBuilder regexwop = SchemaBuilder.struct().name(SCHEMA_NAME_REGEX).optional();
                regexwop.field("regex", Schema.OPTIONAL_STRING_SCHEMA);
                regexwop.field("options", Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(key, regexwop.build());
                break;

            case DOCUMENT:
                SchemaBuilder builderDoc = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                BsonDocument docs = keyValuesforSchema.getValue().asDocument();

                for (Entry<String, BsonValue> doc : docs.entrySet()) {
                    addFieldSchema(doc, builderDoc);
                }
                builder.field(key, builderDoc.build());
                break;

            case ARRAY:
                if (keyValuesforSchema.getValue().asArray().isEmpty()) {
                    // ignore empty arrays; currently only for Avro, but might be worth doing in general as we
                    // cannot conclude an element type in any meaningful way
                    if (sanitizeValue) {
                        return;
                    }
                    switch (arrayEncoding) {
                        case ARRAY:
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
                            break;
                        case DOCUMENT:
                            builder.field(key, SchemaBuilder.struct().name(builder.name() + "." + key).optional().build());
                            break;
                    }
                }
                else {
                    switch (arrayEncoding) {
                        case ARRAY:
                            BsonArray value = keyValuesforSchema.getValue().asArray();
                            BsonType valueType = value.get(0).getBsonType();
                            testType(builder, key, keyValuesforSchema.getValue(), valueType);
                            builder.field(key, SchemaBuilder.array(subSchema(builder, key, valueType, value)).optional().build());
                            break;
                        case DOCUMENT:
                            final BsonArray array = keyValuesforSchema.getValue().asArray();
                            final SchemaBuilder arrayStructBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
                            final Map<String, BsonValue> convertedArray = new HashMap<>();
                            for (int i = 0; i < array.size(); i++) {
                                convertedArray.put(arrayElementStructName(i), array.get(i));
                            }
                            convertedArray.entrySet().forEach(x -> addFieldSchema(x, arrayStructBuilder));
                            builder.field(key, arrayStructBuilder.build());
                            break;
                    }
                }
                break;
            default:
                break;
        }
    }

    private Schema convertSchemaToFloat(String collectionName, String key, Schema originalSchema) {
        Schema convertedSchema = originalSchema;
        final String fullyQualifiedName = fieldFullyQualifiedName(collectionName, key);
        if (this.fieldsToFloat.contains(fullyQualifiedName)) {
            convertedSchema = Schema.OPTIONAL_FLOAT64_SCHEMA;
        }
        return convertedSchema;
    }

    private Object convertValueToFloat(String collectionName, String key, Object originalValue) {
        Object convertedValue = originalValue;
        final String fullyQualifiedName = fieldFullyQualifiedName(collectionName, key);
        if (this.fieldsToFloat.contains(fullyQualifiedName)) {
            convertedValue = Double.valueOf((Integer) originalValue);
        }
        return convertedValue;
    }

    private String fieldFullyQualifiedName(String collectionName, String key) {
        String fullyQualifiedName = key;
        if (collectionName != null) {
            final ArrayList<String> pathTokens = new ArrayList<>(Arrays.asList(collectionName.split("\\.")));
            pathTokens.remove(0); // removing connection name from path
            final String collectionNameWithoutConnection = String.join(".", pathTokens);
            fullyQualifiedName = String.join(".", collectionNameWithoutConnection, key);
        }
        return fullyQualifiedName;
    }
}
