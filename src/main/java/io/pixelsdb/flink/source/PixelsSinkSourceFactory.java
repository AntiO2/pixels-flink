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

 import io.pixelsdb.flink.config.PixelsSinkSourceConfig;
 import org.apache.flink.api.common.serialization.DeserializationSchema;
 import org.apache.flink.configuration.ConfigOption;
 import org.apache.flink.configuration.ReadableConfig;
 import org.apache.flink.table.connector.format.DecodingFormat;
 import org.apache.flink.table.connector.source.DynamicTableSource;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.factories.DeserializationFormatFactory;
 import org.apache.flink.table.factories.DynamicTableSourceFactory;
 import org.apache.flink.table.factories.FactoryUtil;
 import org.apache.flink.table.types.DataType;

 import java.util.HashSet;
 import java.util.Set;

 import static io.pixelsdb.flink.config.PixelsSinkSourceConfig.*;

 public class PixelsSinkSourceFactory implements DynamicTableSourceFactory
 {
     private static String IDENTIFIER = "pixels-sink";
     private final Set<ConfigOption<?>> requiredOptions;
     private final Set<ConfigOption<?>> optionalOptions;

     public PixelsSinkSourceFactory()
     {
         requiredOptions = new HashSet<>();
         requiredOptions.add(HOSTNAME);
         requiredOptions.add(PORT);
         requiredOptions.add(PixelsSinkSourceConfig.DATABASE);
         requiredOptions.add(PixelsSinkSourceConfig.TABLE);
         requiredOptions.add(FactoryUtil.FORMAT);

         optionalOptions = new HashSet<>();
         optionalOptions.add(PixelsSinkSourceConfig.BUCKETS);
     }

     @Override
     public DynamicTableSource createDynamicTableSource(Context context)
     {
         final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
         helper.validate();

         final ReadableConfig options = helper.getOptions();
         final String hostname = options.get(HOSTNAME);
         final int port = options.get(PORT);
         final String tableName = options.get(TABLE);
         final String buckets = options.get(BUCKETS);
         final String database = options.get(DATABASE);

         final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

         final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                 DeserializationFormatFactory.class,
                 FactoryUtil.FORMAT);

         return new PixelsSinkTableSource(hostname, port, database, tableName, buckets, decodingFormat, producedDataType);
     }

     @Override
     public String factoryIdentifier()
     {
         return IDENTIFIER;
     }

     @Override
     public Set<ConfigOption<?>> requiredOptions()
     {
         return requiredOptions;
     }

     @Override
     public Set<ConfigOption<?>> optionalOptions()
     {
         return optionalOptions;
     }
 }
