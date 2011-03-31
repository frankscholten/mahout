/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public interface SerializableConfiguration<T> extends Writable {

  /**
   * Retrieves the Mahout specific configuration type T from the Hadoop {@link Configuration}
   *
   * @param configuration hadoop configuration with serialized Mahout specific configuration
   * @return Mahout specific configuration
   *
   * @throws java.io.IOException when deserialization fails
   */
  T getFromConfiguration(Configuration configuration) throws IOException;

  /**
   * Serializes the Mahout specific configuration type into the Hadoop {@link Configuration}
   *
   * @return Hadoop {@link Configuration}
   *
   * @throws java.io.IOException when serialization fails
   */
  Configuration serializeInConfiguration() throws IOException;

}
