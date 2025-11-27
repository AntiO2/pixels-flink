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
 import org.apache.flink.api.common.functions.OpenContext;
 import org.apache.flink.api.common.functions.RichMapFunction;
 import org.apache.flink.configuration.Configuration;
 import org.apache.flink.table.data.GenericRowData;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.data.TimestampData;
 import org.apache.flink.table.data.StringData;

 import com.google.protobuf.InvalidProtocolBufferException;
 import org.apache.flink.types.RowKind;

 public class ProtobufToRowDataMapFunction extends RichMapFunction<SinkProto.RowRecord, RowData> {

     @Override
     public RowData map(SinkProto.RowRecord rowRecord) throws Exception {
         // 2. 根据 OperationType 确定 RowKind，并设置在 RowData 上
         RowKind rowKind = getRowKindFromOperationType(rowRecord.getOp());

         int filedCount = getFiledCountFromRowRecord(rowRecord);

         // 3. 创建 GenericRowData 对象
         GenericRowData row = new GenericRowData(filedCount);
         row.setRowKind(rowKind);

         // 4. 字段映射 (元数据)

         // TODO: 实际的业务逻辑应该在这里解析 rowRecord.getAfter().getValuesList()
         //       并将其内容映射到 RowData 的后续字段中，这需要列的类型信息(传入iceberg schema info)。

         return row;
     }

     private int getFiledCountFromRowRecord(SinkProto.RowRecord rowRecord)
     {

         // TODO
         return -1;
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