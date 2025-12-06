 /*
  * Copyright 2025 PixelsDB.
  *
  * This file is part of Pixels.
  *
  * Pixels is free software: you can redistribute it and/or modify
  * it under the terms of the Affero GNU General Public License as
  * published by the Free Software Foundation, either version 3 of
  * the License, or (at your option) any later version.
  *
  * Pixels is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * Affero GNU General Public License for more details.
  *
  * You should have received a copy of the Affero GNU General Public
  * License along with Pixels.  If not, see
  * <https://www.gnu.org/licenses/>.
  */

 package io.pixelsdb.flink;

 import com.google.protobuf.ByteString;
 import io.pixelsdb.flink.config.PixelsFlinkConfig;
 import io.pixelsdb.pixels.sink.SinkProto;
 import org.apache.flink.api.common.functions.OpenContext;
 import org.apache.flink.api.common.functions.RichFlatMapFunction;
 import org.apache.flink.api.common.functions.RichMapFunction;
 import org.apache.flink.configuration.Configuration;
 import org.apache.flink.table.data.GenericRowData;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.data.TimestampData;
 import org.apache.flink.table.data.StringData;

 import org.apache.flink.table.data.StringData;
 import org.apache.flink.table.types.logical.LogicalType;
 import org.apache.flink.table.types.logical.LogicalTypeRoot;
 import com.google.protobuf.ByteString;

 import java.nio.ByteBuffer;
 import java.nio.ByteOrder;
 import java.nio.charset.StandardCharsets;
 import java.math.BigDecimal;
 import java.util.Base64;
 import org.apache.flink.table.types.logical.LogicalType;
 import org.apache.flink.table.types.logical.RowType;
 import org.apache.flink.types.RowKind;
 import org.apache.flink.util.Collector;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.util.List;

 public class ProtobufToRowDataMapFunction extends RichFlatMapFunction<SinkProto.RowRecord, RowData> {

     private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufToRowDataMapFunction.class);

     RowType rowType;

     public ProtobufToRowDataMapFunction(PixelsFlinkConfig config)
     {

     }

     @Override
     public void flatMap(SinkProto.RowRecord rowRecord, Collector<RowData> collector) throws Exception {
         SinkProto.OperationType op = rowRecord.getOp();
         RowKind rowKind = getRowKindFromOperationType(op);

         SinkProto.RowValue rowValue;
         switch (op) {
             case INSERT:
             case SNAPSHOT:
                 if (rowRecord.hasAfter()) {
                     collector.collect(convert(rowRecord.getAfter(), RowKind.INSERT));
                 }
                 break;
             case UPDATE:
                 if (rowRecord.hasBefore()) {
                     collector.collect(convert(rowRecord.getBefore(), RowKind.UPDATE_BEFORE));
                 }
                 if (rowRecord.hasAfter()) {
                     collector.collect(convert(rowRecord.getAfter(), RowKind.UPDATE_AFTER));
                 }
                 break;
             case DELETE:
                 if (rowRecord.hasBefore()) {
                     collector.collect(convert(rowRecord.getBefore(), RowKind.DELETE));
                 }
                 break;
             default:
                 LOGGER.warn("Unknown operation type: {}", op);
         }

     }

     private RowData convert(SinkProto.RowValue rowValue, RowKind kind) {
         List<SinkProto.ColumnValue> values = rowValue.getValuesList();
         int arity = rowType.getFieldCount();
         GenericRowData row = new GenericRowData(kind, arity);
         List<LogicalType> fieldTypes = rowType.getChildren();

         // Assuming the order of values in RowValue matches the schema definition
         for (int i = 0; i < arity; i++) {
             if (i < values.size()) {
                 ByteString byteString = values.get(i).getValue();
                 LogicalType type = fieldTypes.get(i);
                 row.setField(i, parseValue(byteString, type));
             } else {
                 // Missing value, set null or handle error
                 row.setField(i, null);
             }
         }
         return row;
     }

     private Object parseValue(ByteString byteString, LogicalType type) {
         // If the ByteString is null or empty, return null for the field value.
         if (byteString == null || byteString.isEmpty()) {
             return null;
         }

         // Convert to byte array and wrap in a ByteBuffer for reading numeric types.
         byte[] bytes = byteString.toByteArray();
         ByteBuffer buffer = ByteBuffer.wrap(bytes);

         // Set to Big Endian (Network Byte Order) to match the expected serialization format
         // from ByteBuffer.putXxx().array() calls, which often defaults to Big Endian.
         buffer.order(ByteOrder.BIG_ENDIAN);

         LogicalTypeRoot typeRoot = type.getTypeRoot();

         switch (typeRoot) {
             // --- Text/String Types ---
             case CHAR:
             case VARCHAR:
             {
                 // CHAR and VARCHAR were stored as UTF-8 strings in ByteString.
                 String value = byteString.toString(StandardCharsets.UTF_8);
                 // Flink's internal representation for strings.
                 return StringData.fromString(value);
             }

             case DECIMAL:
             {
                 // DECIMAL was serialized as a UTF-8 String representation.
                 String decimalString = byteString.toString(StandardCharsets.UTF_8);
                 // Note: Flink internally uses DecimalData, but we return BigDecimal here,
                 // which needs further adaptation by the caller if necessary.
                 return new BigDecimal(decimalString);
             }

             case BINARY:
             case VARBINARY:
             {
                 return byteString.toByteArray();
             }

             // --- Integer Types (4 Bytes) ---
             case INTEGER:
             case DATE: // DATE in Flink is stored as the number of days since Epoch (INT)
             {
                 if (buffer.remaining() < Integer.BYTES) {
                     throw new IllegalArgumentException("Invalid byte length for INT/DATE.");
                 }
                 // Read 4 bytes as an integer.
                 return buffer.getInt();
             }

             // --- Long Types (8 Bytes) ---
             case BIGINT:
             case TIMESTAMP_WITHOUT_TIME_ZONE:
             {
                 if (buffer.remaining() < Long.BYTES) {
                     throw new IllegalArgumentException("Invalid byte length for BIGINT/TIMESTAMP.");
                 }
                 // Read 8 bytes as a long.
                 return buffer.getLong();
             }

             // --- Floating Point Types ---
             case FLOAT:
             {
                 if (buffer.remaining() < Float.BYTES) {
                     throw new IllegalArgumentException("Invalid byte length for FLOAT.");
                 }
                 // Read 4 bytes as the integer bit pattern, then convert back to float.
                 int intBits = buffer.getInt();
                 return Float.intBitsToFloat(intBits);
             }

             case DOUBLE:
             {
                 if (buffer.remaining() < Double.BYTES) {
                     throw new IllegalArgumentException("Invalid byte length for DOUBLE.");
                 }
                 // Read 8 bytes as the long bit pattern, then convert back to double.
                 long longBits = buffer.getLong();
                 return Double.longBitsToDouble(longBits);
             }

             case BOOLEAN:
             {
                 // Assuming boolean was stored as "true" or "false" string representation
                 // in the absence of explicit byte-based serialization logic for BOOLEAN.
                 String value = byteString.toStringUtf8();
                 return Boolean.parseBoolean(value);
             }

             default:
                 throw new UnsupportedOperationException("Unsupported type for deserialization: " + typeRoot);
         }
     }

     private RowKind getRowKindFromOperationType(SinkProto.OperationType op) {
         switch (op) {
             case INSERT:
             case SNAPSHOT:
                 return RowKind.INSERT;
             case UPDATE:
                 return RowKind.UPDATE_AFTER;
             case DELETE:
                 return RowKind.DELETE;
             case UNRECOGNIZED:
             default:
                 System.err.println("Unrecognized OperationType: " + op.name());
                 return RowKind.INSERT;
         }
     }
 }