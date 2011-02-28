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

package org.apache.mahout.clustering.canopy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Tests constructor, getters and setters and default values for the {@link CanopyConfiguration}
 */
public class CanopyConfigurationTest {

  protected Configuration configuration;
  protected Path input;
  protected Path outputBase;

  @Before
  public void setup() {
    configuration = new Configuration();
    input = new Path("input");
    outputBase = new Path("outputBase");
  }

  @Test
  public void testConstructor() {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(configuration, input, outputBase);

    assertEquals(configuration, canopyConfiguration.getConfiguration());
    assertEquals(input, canopyConfiguration.getInputPath());
    assertEquals(outputBase, canopyConfiguration.getBaseOutputPath());

    assertEquals(CanopyConfiguration.DEFAULT_T1, canopyConfiguration.getT1());
    assertEquals(CanopyConfiguration.DEFAULT_T2, canopyConfiguration.getT2());
    assertEquals(CanopyConfiguration.DEFAULT_DISTANCE_MEASURE_CLASSNAME, canopyConfiguration.getDistanceMeasureClassName());
    assertEquals("outputBase", canopyConfiguration.getBaseOutputPath().toString());
    assertEquals("outputBase/clusters-0", canopyConfiguration.getCanopyOutputPath().toString());
    assertEquals("outputBase/clusteredPoints", canopyConfiguration.getPointsOutputPath().toString());
  }

  @Test(expected = NullPointerException.class)
  public void testConstructor_nullOutputBase() {
    new CanopyConfiguration(configuration, input, null);
  }

  @Test
  public void testSetters() {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(configuration, input, outputBase);

    double t1 = 0.8;
    double t2 = 0.2;
    EuclideanDistanceMeasure distanceMeasure = new EuclideanDistanceMeasure();
    boolean runsClustering = false;
    Configuration newConfiguration = new Configuration();

    canopyConfiguration.setConfiguration(newConfiguration);
    canopyConfiguration.setT1(t1);
    canopyConfiguration.setT2(t2);
    canopyConfiguration.setDistanceMeasure(distanceMeasure);
    canopyConfiguration.setRunsClustering(runsClustering);

    assertEquals(newConfiguration, canopyConfiguration.getConfiguration());
    assertEquals(t1, canopyConfiguration.getT1());
    assertEquals(t2, canopyConfiguration.getT2());
    assertEquals(distanceMeasure.getClass().getName(), canopyConfiguration.getDistanceMeasureClassName());
    assertEquals(runsClustering, canopyConfiguration.runsClustering());
  }

  @Test
  public void testGetFromConfiguration() throws IOException {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(configuration, input, outputBase);

    Configuration serialized = canopyConfiguration.serializeInConfiguration();

    CanopyConfiguration deserialized = new CanopyConfiguration().getFromConfiguration(serialized);

    assertEquals(canopyConfiguration, deserialized);
  }

  @Test
  public void testSetters_nullConfiguration() {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(null, input, outputBase);

    assertNotNull(canopyConfiguration.getConfiguration());
  }

  @Test(expected = NullPointerException.class)
  public void testSetters_nullInput() {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(configuration, input, outputBase);

    canopyConfiguration.setInputPath(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSetters_nullDistanceMeasure() {
    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(configuration, input, outputBase);

    canopyConfiguration.setDistanceMeasure(null);
  }
}
