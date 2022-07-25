/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.danghuutoan.debebezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.github.danghuutoan.debezium.transforms.ExtractRecordDiff;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;

/**
 * @author Jiri Pechanec
 */
public class ExtractRecordDiffTest {

  final Schema recordSchema = SchemaBuilder.struct()
      .field("id", Schema.INT8_SCHEMA)
      .field("name", Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  final Schema sourceSchema = SchemaBuilder.struct()
      .field("lsn", Schema.INT32_SCHEMA)
      .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  final Envelope envelope = Envelope.defineSchema()
      .withName("dummy.Envelope")
      .withRecord(recordSchema)
      .withSource(sourceSchema)
      .build();

  private SourceRecord createDeleteRecord() {
    final Schema deleteSourceSchema = SchemaBuilder.struct()
        .field("lsn", SchemaBuilder.int32())
        .field("version", SchemaBuilder.string())
        .build();

    Envelope deleteEnvelope = Envelope.defineSchema()
        .withName("dummy.Envelope")
        .withRecord(recordSchema)
        .withSource(deleteSourceSchema)
        .build();

    final Struct before = new Struct(recordSchema);
    final Struct source = new Struct(deleteSourceSchema);

    before.put("id", (byte) 1);
    before.put("name", "myRecord");
    source.put("lsn", 1234);
    source.put("version", "version!");
    final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
  }

  private SourceRecord createCreateRecord() {
    final Struct before = new Struct(recordSchema);
    final Struct source = new Struct(sourceSchema);

    before.put("id", (byte) 1);
    before.put("name", "myRecord");
    source.put("lsn", 1234);
    source.put("ts_ms", 12836);
    final Struct payload = envelope.create(before, source, Instant.now());
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
  }

  private SourceRecord createUpdateRecord(Object old_value, Object new_value) {
    final Struct before = new Struct(recordSchema);
    final Struct after = new Struct(recordSchema);
    final Struct source = new Struct(sourceSchema);
    final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

    before.put("id", (byte) 1);
    before.put("name", (String) old_value);
    after.put("id", (byte) 1);
    after.put("name", (String) new_value);
    source.put("lsn", 1234);
    transaction.put("id", "571");
    transaction.put("total_order", 42L);
    transaction.put("data_collection_order", 42L);
    final Struct payload = envelope.update(before, after, source, Instant.now());
    payload.put("transaction", transaction);
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
  }

  private SourceRecord createUnknownRecord() {
    final Schema recordSchema = SchemaBuilder.struct().name("unknown")
        .field("id", SchemaBuilder.int8())
        .build();
    final Struct before = new Struct(recordSchema);
    before.put("id", (byte) 1);
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
  }

  private SourceRecord createUnknownUnnamedSchemaRecord() {
    final Schema recordSchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.int8())
        .build();
    final Struct before = new Struct(recordSchema);
    before.put("id", (byte) 1);
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
  }

  @Test
  public void testIgnoreUnknownRecord() {
    try (final ExtractRecordDiff<SourceRecord> transform = new ExtractRecordDiff<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord unknownRecord = createUnknownRecord();
      assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

      final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
      assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
    }
  }

  @Test
  public void testHandleDeleteRecord() {
    try (final ExtractRecordDiff<SourceRecord> transform = new ExtractRecordDiff<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord deleteRecord = createDeleteRecord();
      final SourceRecord unwrapped = transform.apply(deleteRecord);
      assertThat(((Struct) unwrapped.value()).getArray("changed_fields")).hasSize(0);
    }
  }

  @Test
  public void testUnwrapCreateRecord() {
    try (final ExtractRecordDiff<SourceRecord> transform = new ExtractRecordDiff<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord createRecord = createCreateRecord();
      final SourceRecord unwrapped = transform.apply(createRecord);
      assertThat(((Struct) unwrapped.value()).getArray("changed_fields")).hasSize(0);
    }
  }

  @Test
  public void testUnwrapUpdateRecord() {
    try (final ExtractRecordDiff<SourceRecord> transform = new ExtractRecordDiff<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord createRecord = createUpdateRecord("old_value", "new_value");
      final SourceRecord unwrapped = transform.apply(createRecord);
      assertThat(((Struct) unwrapped.value()).getArray("changed_fields")).isEqualTo(new ArrayList<String>() {
        {
          add("name");
        }
      });
    }
  }

}
