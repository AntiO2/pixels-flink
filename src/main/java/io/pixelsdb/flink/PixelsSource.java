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

 import io.pixelsdb.pixels.sink.SinkProto;
 import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
 import org.apache.flink.table.data.RowData;

 import java.util.concurrent.atomic.AtomicBoolean;

 public class PixelsSource implements SourceFunction<SinkProto.RowRecord>
 {
    AtomicBoolean running = new AtomicBoolean(true);
     public PixelsSource(String serverHost, Integer serverPort)
     {
         // open rpc server

     }


     @Override
     public void run(SourceContext<SinkProto.RowRecord> sourceContext) throws Exception
     {
         // loop running rpc client
         while (running.get())
         {
             SinkProto.RowRecord rowRecord = SinkProto.RowRecord.newBuilder().build();
             sourceContext.collect(rowRecord);
         }
     }

     @Override
     public void cancel()
     {
        running.set(false);
     }
 }
