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

 package io.pixelsdb.flink.source;

 import org.apache.flink.api.common.serialization.DeserializationSchema;
 import org.apache.flink.streaming.api.functions.source.SourceFunction;
 import org.apache.flink.table.connector.ChangelogMode;
 import org.apache.flink.table.connector.format.DecodingFormat;
 import org.apache.flink.table.connector.source.DynamicTableSource;
 import org.apache.flink.table.connector.source.ScanTableSource;
 import org.apache.flink.table.connector.source.SourceFunctionProvider;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.types.DataType;

 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.List;

 public class PixelsSinkTableSource implements ScanTableSource
 {
     private final String hostname;
     private final int port;
     private final String schemaName;
     private final String tableName;
     private final String bucketStr;
     private final List<Integer> buckets;
     private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

     private final DataType producedDataType;

     public PixelsSinkTableSource(String hostname, int port, String database, String tableName, String bucketsStr,
                                  DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                  DataType producedDataType)
     {
         this.hostname = hostname;
         this.port = port;
         this.schemaName = database;
         this.tableName = tableName;
         this.decodingFormat = decodingFormat;
         this.producedDataType = producedDataType;
         this.bucketStr = bucketsStr;
         this.buckets = parseBuckets(bucketsStr);
     }


     @Override
     public ChangelogMode getChangelogMode()
     {
         return decodingFormat.getChangelogMode();
     }

     @Override
     public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext)
     {
         final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                 scanContext,
                 producedDataType);

         final SourceFunction<RowData> sourceFunction = new PixelsRpcSource(
                 hostname,
                 port,
                 schemaName,
                 tableName,
                 buckets,
                 deserializer);

         return SourceFunctionProvider.of(sourceFunction, false);
     }

     @Override
     public DynamicTableSource copy()
     {
         return new PixelsSinkTableSource(hostname, port, schemaName, tableName, bucketStr, decodingFormat, producedDataType);
     }

     @Override
     public String asSummaryString()
     {
         return "Pixels Sink Source";
     }

     private List<Integer> parseBuckets(String buckets)
     {
         if (buckets == null || buckets.trim().isEmpty())
         {
             return Collections.emptyList();
         }

         List<Integer> result = new ArrayList<>();
         for (String part : buckets.split(","))
         {
             part = part.trim();
             if (!part.isEmpty())
             {
                 result.add(Integer.parseInt(part));
             }
         }
         return result;
     }
 }
