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

 package io.pixelsdb.flink.config;

 import org.apache.flink.configuration.ConfigOption;
 import org.apache.flink.configuration.ConfigOptions;

 public class PixelsSinkSourceConfig
 {
     public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("pixels.host")
             .stringType()
             .noDefaultValue();

     public static final ConfigOption<Integer> PORT = ConfigOptions.key("pixels.port")
             .intType()
             .defaultValue(9091);

     public static final ConfigOption<String> DATABASE = ConfigOptions.key("pixels.database")
             .stringType()
             .noDefaultValue();

     public static final ConfigOption<String> TABLE = ConfigOptions.key("pixels.table")
             .stringType()
             .noDefaultValue();

     public static final ConfigOption<String> BUCKETS = ConfigOptions.key("pixels.buckets")
             .stringType()
             .noDefaultValue();
 }
