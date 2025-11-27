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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pixelsdb.flink;

import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.table.data.RowData;

import org.apache.iceberg.catalog.TableIdentifier;


public class PixelsFlinkSinkJob
{

    // 假设您将 Hive Metastore URI 作为一个参数传入
    private static final String HIVE_METASTORE_URI = "hive.metastore.uri";
    private static final String CATALOG_NAME = "catalog.name";


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // --- 1. 参数校验和获取 ---

        // 确保传递了所有必要的参数
        params.getRequired("source");
        String serverHost = params.getRequired("source.server.host");
        int serverPort = params.getInt("source.server.port"); // 可以是可选
        String databaseName = params.getRequired("source.database.name"); // 使用更标准的数据库名
        String tableName = params.getRequired("source.tablename");

        // --- 2. Flink 环境设置 ---

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 推荐设置 Checkpoint，Iceberg Sink 依赖它来保证写入一致性
        env.enableCheckpointing(5000); // 5秒一次 Checkpoint

        // --- 3. 初始化 Iceberg Table 对象 ---

        // Flink Job 需要一个 Table 对象来配置 Sink
        Table table = initializeIcebergTable(
                params.get("catalog.name", "catalog"),
                databaseName,
                tableName
        );

        // --- 4. Source 和 DataStream ---

        DataStream<SinkProto.RowRecord> pixelsRpcSource = env
                .addSource(new PixelsSource(serverHost, serverPort)) // 假设您的 Source 接受 host/port
                .name("pixelsRpcSource");

        DataStream<RowData> pixelsRowDataStream = pixelsRpcSource
                // 使用 ProtobufToRowDataMapFunction 进行转换
                .map(new ProtobufToRowDataMapFunction())
                .name("protobuf-to-rowdata-converter");

        // FlinkSink.forRowData(...).table(table).build()
        // 已经完成了 Sink 的附加，返回 DataStreamSink 实例
        DataStreamSink<RowData> sink = FlinkSink.forRowData(pixelsRowDataStream)
                .table(table)
                // 写入策略和并发度配置（可选，但推荐）
                .overwrite(false) // 默认为 false，设置为 true 需谨慎
                .build();

        // 在返回的 DataStreamSink 对象上设置名称
        sink.name("iceberg-sink");

        env.execute("Iceberg Sink " + databaseName + " " + tableName);
    }

    /**
     * 初始化 Iceberg Table 对象，使用 HiveCatalog 示例
     */
    private static Table initializeIcebergTable(
            String catalogName,
            String databaseName,
            String tableName) {


        GlueCatalog glueCatalog = new GlueCatalog();
        // glueCatalog.initialize();
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);

        if (!glueCatalog.tableExists(tableIdentifier)) {
            throw new IllegalStateException("Iceberg Table not found: " + tableIdentifier);
        }

        return glueCatalog.loadTable(tableIdentifier);
    }
}
