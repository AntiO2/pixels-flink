package io.pixelsdb.flink;

import io.pixelsdb.flink.source.PixelsRpcClient;
import io.pixelsdb.pixels.sink.SinkProto;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPixelsRpcClient
{
    @Test
    public void testPollEvent()
    {
        String host = "realtime-pixels-coordinator";
        String schemaName = "pixels_bench_sf10x";
        String tableName = "transfer";
        int port = 9091;
        List<Integer> buckets = Arrays.asList(0, 1, 2, 3);
        PixelsRpcClient pixelsRpcClient = new PixelsRpcClient(host, port);
        List<SinkProto.RowRecord> recordsList = pixelsRpcClient.pollEvents(schemaName, tableName, buckets);
        for (SinkProto.RowRecord record : recordsList)
        {
            record.getAfter();
        }
    }
}
