/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pixelsdb.flink;

import io.pixelsdb.flink.config.PixelsFlinkConfig;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.flink.table.types.logical.*;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.table.data.RowData;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.List; // 导入 List 用于获取等值字段
import java.util.Set;


public class PixelsFlinkSinkJob
{
    // 定义常量以避免硬编码参数名
    private static final String CATALOG_NAME = "catalog.name";


    public static void main(String[] args) throws Exception {

        PixelsFlinkConfig config = new PixelsFlinkConfig(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        startJob(env, config);




        // 从 Iceberg Table Schema 中获取等值字段 (Equality Fields) 实现 Upsert
        // Iceberg 标识符字段（Identifier Fields）就是用于 Upsert 的键。
//        Set<String> equalityFields = targetTable.schema().identifierFieldNames();
//
//        if (equalityFields != null && !equalityFields.isEmpty()) {
//            System.out.println("Enabling Upsert mode by reading equality fields from Table Schema: " + equalityFields);
//            sinkBuilder.equalityFieldColumns((List<String>) equalityFields);
//        } else {
//            System.out.println("No identifier fields found in Iceberg Table schema. Sink will perform normal append/delete based on RowKind.");
//        }


        // 附加到流环境并构建 Sink
//        DataStreamSink<RowData> sink = sinkBuilder.build();
//
//        // 在返回的 DataStreamSink 对象上设置名称
//        sink.name("iceberg-sink-to-" + configdatabaseName + "." + tableName);

        env.execute("Pixels Flink CDC Sink Job");
    }

    public static void startJob(StreamExecutionEnvironment env, PixelsFlinkConfig config) {
        env.enableCheckpointing(config.checkpointIntervalMs);

        RowType rowType = parseSchema(config.sourceSchema);

        PixelsRpcSource source = new PixelsRpcSource(config);
        DataStream<SinkProto.RowRecord> pixelsRpcStream = env
                .addSource(source)
                .name("PixelsRpcSource");

        DataStream<RowData> pixelsRowDataStream = pixelsRpcStream
                .flatMap(new ProtobufToRowDataMapFunction(config))
                .name("protobuf-to-rowdata-converter");

        if ("iceberg".equalsIgnoreCase(config.sinkType)) {
            configureIcebergSink(pixelsRowDataStream, config);
        } else if ("paimon".equalsIgnoreCase(config.sinkType)) {
            configurePaimonSink(pixelsRowDataStream, config);
        } else {
            throw new IllegalArgumentException("Unsupported sink type: " + config.sinkType);
        }
    }
    private static RowType parseSchema(String schemaStr) {
        String[] columns = schemaStr.split(",");
        List<String> names = new ArrayList<>();
        List<LogicalType> types = new ArrayList<>();

        for (String col : columns) {
            String[] parts = col.trim().split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid schema format. Expected 'name:type', got: " + col);
            }
            String name = parts[0].trim();
            String typeStr = parts[1].trim().toUpperCase();

            names.add(name);
            types.add(mapType(typeStr));
        }

        return RowType.of(
                types.toArray(new LogicalType[0]),
                names.toArray(new String[0])
        );
    }
    private static LogicalType mapType(String typeStr) {
        switch (typeStr) {
            case "INT":
            case "INTEGER":
                return new IntType();
            case "STRING":
            case "VARCHAR":
                return new VarCharType();
            case "BIGINT":
            case "LONG":
                return new BigIntType();
            case "FLOAT":
                return new FloatType();
            case "DOUBLE":
                return new DoubleType();
            case "BOOLEAN":
            case "BOOL":
                return new BooleanType();
            default:
                throw new UnsupportedOperationException("Unsupported type in config: " + typeStr);
        }
    }

    public static void configurePaimonSink(DataStream<RowData> stream, PixelsFlinkConfig config) {
        String tableNameStr = config.paimonTableName;
        if (tableNameStr == null) {
            throw new IllegalArgumentException("Paimon table name must be specified.");
        }
        
        String dbName = config.schemaName;
        String tblName = config.tableName;

        // Build Paimon Options from configuration
        Options options = new Options();
        options.set("catalog-type", config.paimonCatalogType);
        options.set("warehouse", config.paimonCatalogWarehouse);

        try {
            // Create Paimon Catalog
            org.apache.paimon.catalog.Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));

            // Load the target table
            Identifier identifier = Identifier.create(dbName, tblName);
            org.apache.paimon.table.Table table = catalog.getTable(identifier);

            if (table == null) {
                throw new IllegalStateException("Paimon Table not found: " + tableNameStr);
            }

            // Build Flink Sink and attach to the stream
            new FlinkSinkBuilder((FileStoreTable) table)
                    .withInput(stream)
                    .build();

        } catch (Exception e) {
            throw new RuntimeException("Failed to configure Paimon sink for " + tableNameStr, e);
        }
    }

    public static void configureIcebergSink(DataStream<RowData> stream, PixelsFlinkConfig config) {
        String tableNameStr = config.icebergTableName;
        if (tableNameStr == null) {
            throw new IllegalArgumentException("Iceberg table name must be specified.");
        }

        // Parse database and table names
        Map.Entry<String, String> parsedNames = parseDbAndTableName(tableNameStr);
        String dbName = parsedNames.getKey();
        String tblName = parsedNames.getValue();

        // Build catalog properties
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", config.icebergCatalogWarehouse);
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"); // Required for S3 access

        CatalogLoader catalogLoader;
        Configuration hadoopConf = new Configuration();

        // Determine Catalog Loader based on type
        if ("glue".equalsIgnoreCase(config.icebergCatalogType)) {
            // Use AWS Glue Catalog
            catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");

            catalogLoader = CatalogLoader.custom("glue_catalog", catalogProps, hadoopConf,
                    "org.apache.iceberg.aws.glue.GlueCatalog");
        } else {
            throw new RuntimeException("Unsupported Iceberg catalog type: " + config.icebergCatalogType);
        }

        // Load Catalog and Table Identifier
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(dbName, tblName);

        if (!catalog.tableExists(identifier)) {
            throw new IllegalStateException("Iceberg Table not found: " + identifier);
        }

        // Create TableLoader for the sink
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        // Build Flink Sink. It uses RowKind for CDC operations.
        FlinkSink.forRowData(stream)
                .tableLoader(tableLoader)
                .append();
    }
}