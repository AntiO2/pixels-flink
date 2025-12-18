package io.pixelsdb.flink.source;

import io.pixelsdb.flink.format.ChangelogRowRecordDeserializer;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PixelsRpcSource extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData>
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcSource.class);

    private final String host;
    private final int port;
    private final String schemaName;
    private final String tableName;
    private final ChangelogRowRecordDeserializer deserializer;
    private transient PixelsRpcClient client;
    private final List<Integer> buckets;
    private volatile boolean isRunning = true;

    public PixelsRpcSource(String host, int port, String schemaName, String tableName, List<Integer> buckets, DeserializationSchema<RowData> deserializer)
    {
        this.host = host;
        this.port = port;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.buckets = buckets;
        this.deserializer = (ChangelogRowRecordDeserializer) deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception
    {
        super.open(parameters);
        client = new PixelsRpcClient(host, port);
        LOG.info("PixelsRpcSource started for table {}.{} at {}:{}", schemaName, tableName, host, port);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception
    {
        while (isRunning)
        {
            try
            {
                // Poll events
                List<SinkProto.RowRecord> events = client.pollEvents(schemaName, tableName, buckets);
                for (SinkProto.RowRecord event : events)
                {
                    ctx.collect(deserializer.deserialize(event));
                }
            } catch (Exception e)
            {
                LOG.error("Error during polling", e);
            }
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }

    @Override
    public void close() throws Exception
    {
        if (client != null)
        {
            client.close();
        }
        super.close();
    }

    @Override
    public TypeInformation<RowData> getProducedType()
    {
        return deserializer.getProducedType();
    }
}
