/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.danghuutoan.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.transforms.SmtManager;
/**
 * Debezium generates CDC (<code>Envelope</code>) records that are struct of values containing values
 * <code>before</code> and <code>after change</code>. Sink connectors usually are not able to work
 * with a complex structure so a user use this SMT to extract <code>after</code> value and send it down
 * unwrapped in <code>Envelope</code>.
 * <p>
 * The functionality is similar to <code>ExtractField</code> SMT but has a special semantics for handling
 * delete events; when delete event is emitted by database then Debezium emits two messages: a delete
 * message and a tombstone message that serves as a signal to Kafka compaction process.
 * <p>
 * The SMT by default drops the tombstone message created by Debezium and converts the delete message into
 * a tombstone message that can be dropped, too, if required.
 * <p>
 * The SMT also has the option to insert fields from the original record (e.g. 'op' or 'source.ts_ms' into the
 * unwrapped record or ad them as header attributes.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ExtractOldRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String PURPOSE = "source field insertion";
  private static final int SCHEMA_CACHE_SIZE = 64;
  private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");
  private static final Pattern NEW_FIELD_SEPARATOR = Pattern.compile(":");
  private List<FieldReference> additionalFields;
  private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
  private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
  private BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache;
  private SmtManager<R> smtManager;

  @Override
  public void configure(final Map<String, ?> configs) {
    final Configuration config = Configuration.from(configs);
    smtManager = new SmtManager<>(config);

    Map<String, String> delegateConfig = new LinkedHashMap<>();
    delegateConfig.put("field", "before");
    beforeDelegate.configure(delegateConfig);

    delegateConfig = new HashMap<>();
    delegateConfig.put("field", "after");
    afterDelegate.configure(delegateConfig);

    schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
  }

  @Override
  public R apply(final R record) {
    if (record.value() == null) {
      return record;
    }

    if (!smtManager.isValidEnvelope(record)) {
      return record;
    }

    R newRecord = afterDelegate.apply(record);
    R oldRecord = beforeDelegate.apply(record);
    List<String> diffFields = getDiffFields(newRecord, oldRecord);
    return addFields(additionalFields, record, record, diffFields);
  }



  private  List<String> getDiffFields(R unwrappedNewRecord, R unwrappedOldRecord) {
    List<String> diffFields = new ArrayList<>();
    if (unwrappedNewRecord.value() != null && unwrappedOldRecord.value() != null){
      final Struct new_value = requireStruct(unwrappedNewRecord.value(), PURPOSE);
      final Struct old_value = requireStruct(unwrappedOldRecord.value(), PURPOSE);

      for (org.apache.kafka.connect.data.Field field : new_value.schema().fields()) {
        String field_name = field.name();
        Object new_field_value = new_value.get(field_name);
        Object old_field_value = old_value.get(field_name);
        boolean isEqual = new_field_value == null ? old_field_value == null : new_field_value.equals(old_field_value);
        if (isEqual == false)
          diffFields.add(field_name);
      }
    }

    return diffFields;
  }


  private R addFields(List<FieldReference> additionalFields, R originalRecord, R unwrappedRecord, List<String> diffFields) {
    final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
    Struct originalRecordValue = (Struct) originalRecord.value();

    Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(),
            s -> makeUpdatedSchema(value.schema(), originalRecordValue));

    // Update the value with the new fields
    Struct updatedValue = new Struct(updatedSchema);
    for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
      Object field_value = value.get(field);
      if (field_value != null) updatedValue.put(field.name(), field_value);

    }

    updatedValue.put("changed_fields", diffFields);

    return unwrappedRecord.newRecord(
            unwrappedRecord.topic(),
            unwrappedRecord.kafkaPartition(),
            unwrappedRecord.keySchema(),
            unwrappedRecord.key(),
            updatedSchema,
            updatedValue,
            unwrappedRecord.timestamp());
  }

  private Schema makeUpdatedSchema(Schema schema, Struct originalRecordValue) {
    // Get fields from original schema
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (org.apache.kafka.connect.data.Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    SchemaBuilder changedFieldSchema = (SchemaBuilder) SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional();
    builder.field("changed_fields",changedFieldSchema);
    return builder.build();
  }

  @Override
  public ConfigDef config() {
    final ConfigDef config = new ConfigDef();
    Field.group(config, null, ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES,
            ExtractNewRecordStateConfigDefinition.HANDLE_DELETES, ExtractNewRecordStateConfigDefinition.ADD_FIELDS,
            ExtractNewRecordStateConfigDefinition.ADD_HEADERS,
            ExtractNewRecordStateConfigDefinition.ROUTE_BY_FIELD);
    return config;
  }

  @Override
  public void close() {
    beforeDelegate.close();
    afterDelegate.close();
  }

  /**
   * Represents a field that should be added to the outgoing record as a header
   * attribute or struct field.
   */
  private static class FieldReference {

    /**
     * The struct ("source", "transaction") hosting the given field, or {@code null} for "op" and "ts_ms".
     */
    private final String struct;

    /**
     * The simple field name.
     */
    private final String field;

    /**
     * The name for the outgoing attribute/field, e.g. "__op" or "__source_ts_ms" when the prefix is "__"
     */
    private final String newField;

    private FieldReference(String prefix, String field) {
      String[] parts = NEW_FIELD_SEPARATOR.split(field);
      String[] splits = FIELD_SEPARATOR.split(parts[0]);
      this.field = splits.length == 1 ? splits[0] : splits[1];
      this.struct = (splits.length == 1) ? determineStruct(this.field) : splits[0];

      if (parts.length == 1) {
        this.newField = prefix + (splits.length == 1 ? this.field : this.struct + "_" + this.field);
      }
      else if (parts.length == 2) {
        this.newField = prefix + parts[1];
      }
      else {
        throw new IllegalArgumentException("Unexpected field name: " + field);
      }
    }

    /**
     * Determines the struct hosting the given unqualified field.
     */
    private static String determineStruct(String simpleFieldName) {
      if (simpleFieldName.equals(Envelope.FieldName.OPERATION) || simpleFieldName.equals(Envelope.FieldName.TIMESTAMP)) {
        return null;
      }
      else if (simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY) ||
              simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY) ||
              simpleFieldName.equals(TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY)) {
        return Envelope.FieldName.TRANSACTION;
      }
      else {
        return Envelope.FieldName.SOURCE;
      }
    }

    static List<FieldReference> fromConfiguration(String fieldPrefix, String addHeadersConfig) {
      if (Strings.isNullOrEmpty(addHeadersConfig)) {
        return Collections.emptyList();
      }
      else {
        return Arrays.stream(addHeadersConfig.split(","))
                .map(String::trim)
                .map(field -> new FieldReference(fieldPrefix, field))
                .collect(Collectors.toList());
      }
    }

    public String getNewField() {
      return this.newField;
    }

    Object getValue(Struct originalRecordValue) {
      Struct parentStruct = struct != null ? (Struct) originalRecordValue.get(struct) : originalRecordValue;

      // transaction is optional; e.g. not present during snapshotting atm.
      return parentStruct != null ? parentStruct.get(field) : null;
    }

    Schema getSchema(Schema originalRecordSchema) {
      Schema parentSchema = struct != null ? originalRecordSchema.field(struct).schema() : originalRecordSchema;

      org.apache.kafka.connect.data.Field schemaField = parentSchema.field(field);

      if (schemaField == null) {
        throw new IllegalArgumentException("Unexpected field name: " + field);
      }

      return SchemaUtil.copySchemaBasics(schemaField.schema()).optional().build();
    }
  }
}
