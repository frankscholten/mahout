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

package org.apache.mahout.clustering.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.mahout.clustering.kmeans.KMeansConfiguration.DEFAULT_CONVERGENCE_DELTA;
import static org.apache.mahout.clustering.kmeans.KMeansConfiguration.DEFAULT_DISTANCE_MEASURE_CLASSNAME;

/**
 * Tests constructor, getters and setters and default values for the {@link KMeansConfiguration}
 */
public class KMeansConfigurationTest extends MahoutTestCase {

  private Configuration configuration;
  private Path input;
  private Path output;
  private Path clusterPath;
  private int maxIterations;

  @Before
  public void setup() {
    configuration = new Configuration();
    input = new Path("input");
    output = new Path("output");
    clusterPath = new Path("clusters");
    maxIterations = 10;
  }

  @Test
  public void testConstructor_defaultValues() throws IOException {
    KMeansConfiguration kMeansConfiguration = new KMeansConfiguration(configuration, input, output, clusterPath, maxIterations);

    assertSame(configuration, kMeansConfiguration.getConfiguration());
    assertSame(input, kMeansConfiguration.getInputVectors());
    assertSame(output, kMeansConfiguration.getOutputclusters());
    assertEquals(clusterPath, kMeansConfiguration.getInputClusters());
    assertSame(maxIterations, kMeansConfiguration.getMaxIterations());
    assertTrue(kMeansConfiguration.runsClustering());

    assertEquals(DEFAULT_DISTANCE_MEASURE_CLASSNAME, kMeansConfiguration.getDistanceMeasureClassName());
    assertEquals(DEFAULT_CONVERGENCE_DELTA, kMeansConfiguration.getConvergenceDelta(), EPSILON);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_nullConfiguration() throws IOException {
    new KMeansConfiguration(null, input, output, clusterPath, maxIterations);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_nullInput() throws IOException {
    new KMeansConfiguration(configuration, null, output, clusterPath, maxIterations);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_nullOutput() throws IOException {
    new KMeansConfiguration(configuration, input, null, clusterPath, maxIterations);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_nullClusterPath() throws IOException {
    new KMeansConfiguration(configuration, input, output, null, maxIterations);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_negativeMaxIterations() throws IOException {
    new KMeansConfiguration(configuration, input, output, clusterPath, -1);
  }

  @Test
  public void testSetters() throws IOException {
    KMeansConfiguration kMeansConfiguration = new KMeansConfiguration(configuration, input, output, clusterPath, maxIterations);

    double convergenceDelta = 0.8;
    int maxIterations = 5;
    Path output = new Path("clusters/cluster-1");
    CosineDistanceMeasure distanceMeasure = new CosineDistanceMeasure();
    boolean runsClustering = false;

    kMeansConfiguration.setConvergenceDelta(convergenceDelta);
    kMeansConfiguration.setDistanceMeasure(distanceMeasure);
    kMeansConfiguration.setMaxIterations(maxIterations);
    kMeansConfiguration.setOutputClusters(output);
    kMeansConfiguration.setRunClustering(runsClustering);

    assertEquals(convergenceDelta, kMeansConfiguration.getConvergenceDelta(), EPSILON);
    assertEquals(distanceMeasure.getClass().getName(), kMeansConfiguration.getDistanceMeasureClassName());
    assertEquals(distanceMeasure, kMeansConfiguration.getDistanceMeasure());
    assertEquals(maxIterations, kMeansConfiguration.getMaxIterations());
    assertEquals(output, kMeansConfiguration.getOutputclusters());
    assertEquals(runsClustering, kMeansConfiguration.runsClustering());
    assertEquals(new Path(kMeansConfiguration.getOutputclusters().getParent(), AbstractCluster.CLUSTERED_POINTS_DIR), kMeansConfiguration.getOutputPoints());
  }

  @Test
  public void testGetFromConfiguration() throws IOException {
    KMeansConfiguration kMeansConfiguration = new KMeansConfiguration(configuration, input, output, clusterPath, maxIterations);

    Configuration serialized = kMeansConfiguration.serializeInConfiguration();

    KMeansConfiguration deserialized = new KMeansConfiguration().getFromConfiguration(serialized);

    assertEquals(kMeansConfiguration, deserialized);
  }

  @Test(expected = RuntimeException.class)
  public void testGetFromConfiguration_unInstantiatiableDistanceMeasureClass() throws IOException {
    class PrivateDistanceMeasure extends EuclideanDistanceMeasure {
    }

    KMeansConfiguration kMeansConfiguration = new KMeansConfiguration(configuration, input, output, clusterPath, maxIterations);
    kMeansConfiguration.setDistanceMeasure(new PrivateDistanceMeasure());

    Configuration serialized = kMeansConfiguration.serializeInConfiguration();

    KMeansConfiguration deserialized = new KMeansConfiguration().getFromConfiguration(serialized);

    assertEquals(kMeansConfiguration, deserialized);
  }
}
