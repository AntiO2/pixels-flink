package io.pixelsdb.flink;

import com.google.protobuf.ByteString;
import io.pixelsdb.flink.config.PixelsFlinkConfig;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class PixelsRpcSource extends RichSourceFunction<SinkProto.RowRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcSource.class);

    private final String host;
    private final int port;
    private final String schemaName;
    private final String tableName;

    private transient PixelsRpcClient client;
    private volatile boolean isRunning = true;

    public PixelsRpcSource(PixelsFlinkConfig config) {
        this.host = config.pixelsServerHost;
        this.port = config.pixelsServerPort;
        this.schemaName = config.schemaName;
        this.tableName = config.tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new PixelsRpcClient(host, port);
        LOG.info("PixelsRpcSource started for table {}.{} at {}:{}", schemaName, tableName, host, port);
    }

    @Override
    public void run(SourceContext<SinkProto.RowRecord> ctx) throws Exception {
        while (isRunning) {
            try {
                // Poll events
                List<SinkProto.RowRecord> events= client.pollEvents(schemaName, tableName);

                for (SinkProto.RowRecord event : events) {
                    ctx.collect(event);
                }
            } catch (Exception e) {
                LOG.error("Error during polling", e);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
        super.close();
    }
}
