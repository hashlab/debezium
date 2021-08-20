/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.MongoDbFieldName;
import io.debezium.data.Envelope.FieldName;
import io.debezium.schema.FieldNameSelector;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;
import io.debezium.transforms.SmtManager;

/**
 * Extends from ExtractNewDocumentState and adds schema and type casting from int to float given a field list in
 * debezium.source.transforms.unwrap.field.tofloat.list conf in application.properties.
 */
public class HashExtractNewDocumentState<R extends ConnectRecord<R>> extends ExtractNewDocumentState<R> {

    public static final Field FIELD_TOFLOAT = Field.create("field.tofloat.list")
            .withDisplayName("Convert Fields to Float")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("A comma-separated list of fully-qualified names of fields that should be casted to float");

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null,
                ARRAY_ENCODING,
                FLATTEN_STRUCT,
                DELIMITER,
                SANITIZE_FIELD_NAMES,
                FIELD_TOFLOAT);
        return config;
    }

    @Override
    public void configure(final Map<String, ?> map) {
        final Configuration config = Configuration.from(map);
        smtManager = new SmtManager<>(config);

        final Field.Set configFields = Field.setOf(ARRAY_ENCODING, FLATTEN_STRUCT, DELIMITER,
                OPERATION_HEADER,
                ADD_SOURCE_FIELDS,
                ExtractNewRecordStateConfigDefinition.HANDLE_DELETES,
                ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES,
                ExtractNewRecordStateConfigDefinition.ADD_HEADERS,
                ExtractNewRecordStateConfigDefinition.ADD_FIELDS,
                SANITIZE_FIELD_NAMES,
                FIELD_TOFLOAT);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        converter = new HashMongoDataConverter(
                ArrayEncoding.parse(config.getString(ARRAY_ENCODING)),
                FieldNameSelector.defaultNonRelationalSelector(config.getBoolean(SANITIZE_FIELD_NAMES)),
                config.getString(FIELD_TOFLOAT).split(","),
                config.getBoolean(SANITIZE_FIELD_NAMES));

        addOperationHeader = config.getBoolean(OPERATION_HEADER);

        addSourceFields = determineAdditionalSourceField(config.getString(ADD_SOURCE_FIELDS));

        addFieldsPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS_PREFIX);
        String addHeadersPrefix = config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS_PREFIX);
        additionalHeaders = FieldReference.fromConfiguration(addHeadersPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_HEADERS));
        additionalFields = FieldReference.fromConfiguration(addFieldsPrefix, config.getString(ExtractNewRecordStateConfigDefinition.ADD_FIELDS));

        flattenStruct = config.getBoolean(FLATTEN_STRUCT);
        delimiter = config.getString(DELIMITER);

        dropTombstones = config.getBoolean(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES));

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", FieldName.AFTER);
        final Map<String, String> patchExtractorConfig = new HashMap<>();
        patchExtractorConfig.put("field", MongoDbFieldName.PATCH);
        final Map<String, String> keyExtractorConfig = new HashMap<>();
        keyExtractorConfig.put("field", "id");

        afterExtractor.configure(afterExtractorConfig);
        patchExtractor.configure(patchExtractorConfig);
        keyExtractor.configure(keyExtractorConfig);

        final Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("delimiter", delimiter);
        recordFlattener.configure(delegateConfig);

    }
}
