package io.pixelsdb.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

/**
 * Configuration class for Pixels Flink Jobs.
 * This class uses Flink's ParameterTool to parse command line arguments
 * and provides immutable access to all configuration parameters.
 */
public class PixelsFlinkConfig implements Serializable {

    // ==========================================
    // Pixels Server Configuration
    // ==========================================
    public final String pixelsServerHost;
    public final int pixelsServerPort;

    // ==========================================
    // Schema Definition
    // ==========================================
    public final String schemaName;
    public final String tableName;
    public final String sourceSchema; // Format: col1:TYPE,col2:TYPE

    // ==========================================
    // Sink Configuration
    // ==========================================
    public final String sinkType; // Supported: paimon, iceberg

    // ==========================================
    // Paimon Configuration
    // ==========================================
    public final String paimonCatalogType;
    public final String paimonCatalogWarehouse;
    public final String paimonTableName;

    // ==========================================
    // Iceberg Configuration
    // ==========================================
    public final String icebergCatalogType;
    public final String icebergCatalogWarehouse;
    public final String icebergTableName;

    // ==========================================
    // Flink Job Configuration
    // ==========================================
    public final long checkpointIntervalMs;

    /**
     * Constructs the configuration by parsing the command line arguments using ParameterTool.
     * Arguments should be passed in the format: --key value
     *
     * @param args The command line arguments (String[] args from main method).
     */
    public PixelsFlinkConfig(String[] args) {
        // 1. Parse arguments into a ParameterTool instance
        final ParameterTool params = ParameterTool.fromArgs(args);

        // ==========================================
        // Pixels Server Configuration
        // Use getRequired() for mandatory parameters, or get() with a default value.
        // ==========================================
        this.pixelsServerHost = params.getRequired("pixels.server.host");
        // Use getInt() for numeric types, with a sensible default value
        this.pixelsServerPort = params.getInt("pixels.server.port", 50051);

        // ==========================================
        // Schema Definition
        // ==========================================
        this.schemaName = params.get("schema.name", "public");
        this.tableName = params.getRequired("table.name");
        this.sourceSchema = params.getRequired("source.schema");

        // ==========================================
        // Sink Configuration
        // ==========================================
        this.sinkType = params.getRequired("sink.type");

        // ==========================================
        // Paimon Configuration
        // ==========================================
        this.paimonCatalogType = params.get("paimon.catalog.type", "paimon");
        this.paimonCatalogWarehouse = params.get("paimon.catalog.warehouse");
        this.paimonTableName = params.get("paimon.table.name");

        // ==========================================
        // Iceberg Configuration
        // ==========================================
        this.icebergCatalogType = params.get("iceberg.catalog.type", "glue");
        this.icebergCatalogWarehouse = params.get("iceberg.catalog.warehouse");
        this.icebergTableName = params.get("iceberg.table.name");

        // ==========================================
        // Flink Job Configuration
        // ==========================================
        this.checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 10000L);
    }

    @Override
    public String toString() {
        return "PixelsFlinkConfig{" +
                "pixelsServerHost='" + pixelsServerHost + '\'' +
                ", pixelsServerPort=" + pixelsServerPort +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", sourceSchema='" + sourceSchema + '\'' +
                ", sinkType='" + sinkType + '\'' +
                ", paimonCatalogType='" + paimonCatalogType + '\'' +
                ", paimonCatalogWarehouse='" + paimonCatalogWarehouse + '\'' +
                ", paimonTableName='" + paimonTableName + '\'' +
                ", icebergCatalogType='" + icebergCatalogType + '\'' +
                ", icebergCatalogWarehouse='" + icebergCatalogWarehouse + '\'' +
                ", icebergTableName='" + icebergTableName + '\'' +
                ", checkpointIntervalMs=" + checkpointIntervalMs +
                '}';
    }
}