package io.pixelsdb.flink;

import io.pixelsdb.flink.config.PixelsSinkSourceConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class TestPixelsRpcSourceTableApi {
    static String host = "realtime-pixels-coordinator";
    static int port = 9091;
    static String schemaName = "pixels_bench_sf10x";
    static String tableName = "checking";
    static String bucketStr = "0,3";

    public static void main(String[] args)  throws Exception {
        testSourceRegistrationAndRead();
    }


    static void testSourceRegistrationAndRead() throws Exception {
        // 1. Set up the Flink Streaming Execution Environment (Local Mode)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a StreamTableEnvironment based on the streaming environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 2. Define the Table Schema based on the provided DDL
        Schema tableSchema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("sourceid", DataTypes.INT())
                .column("targetid", DataTypes.INT())
                .column("amount", DataTypes.FLOAT())
                .column("type", DataTypes.CHAR(10)) // CHAR(10)
                .column("timestamp", DataTypes.TIMESTAMP(6)) // TIMESTAMP without precision (defaults to TIMESTAMP(6))
                .column("freshness_ts", DataTypes.TIMESTAMP(3))
                .build();

        // 3. Define the Table Descriptor (Connector and Format Properties)
        TableDescriptor sourceDescriptor = TableDescriptor.forConnector("pixels-sink") // Target Connector Factory
                .schema(tableSchema)
                .option("format", "pixels-rowrecord") // Target Format Factory

                // Add custom Connector properties (corresponding to the WITH clause)
                .option(PixelsSinkSourceConfig.HOSTNAME, host)
                .option(PixelsSinkSourceConfig.PORT, 9091)
                .option(PixelsSinkSourceConfig.DATABASE, schemaName)
                .option(PixelsSinkSourceConfig.TABLE, tableName)
                .option(PixelsSinkSourceConfig.BUCKETS, bucketStr)
                .build();

        // 4. Create the Temporary Table (Register the Descriptor with Table Environment)
        String tableName = "pixels_source_table";
        // Assert that the registration and underlying Factory discovery do not throw an exception
        tEnv.createTemporaryTable(tableName, sourceDescriptor);

        // 5. Define a Print Sink to receive and output data during the test
        TableDescriptor printDescriptor = TableDescriptor.forConnector("print")
                .schema(tableSchema)
                .build();
        tEnv.createTemporaryTable("print_sink", printDescriptor);

        // 6. Execute the DML: Read from the Source table and insert into the Print Sink
        TableResult result = tEnv.from(tableName)
                .executeInsert("print_sink");

        // 7. Verification: Check job submission and cancellation
        try {
            result.await();
            // Verify that the job was successfully submitted
            result.getJobClient().ifPresent(client -> {
                System.out.println("Flink Job submitted successfully with ID: " + client.getJobID());
            });

            // For a proper test, you would call result.await() here
            // and then verify the contents of the print sink output or another test sink.

        } catch (Exception e) {
            // Catch any job execution failure (not factory instantiation failure)
            throw new RuntimeException("Flink job failed during execution.", e);
        } finally {
            // Important: Cancel the job client to stop the continuous streaming job
            result.getJobClient().ifPresent(client -> client.cancel());
        }
    }
}
