/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;

/**
 * Holds all the configuration for the {@link CanopyMapReduceAlgorithm} and {@link CanopySequentialAlgorithm}
 */
public class CanopyConfiguration {

  // Hadoop configuration keys
  public static final String INPUT_KEY = "org.apache.mahout.clustering.canopy.input";
  public static final String OUTPUT_BASE_KEY = "org.apache.mahout.clustering.canopy.output";
  public static final String POINTS_PATH_KEY = "org.apache.mahout.clustering.canopy.output.points";
  public static final String CANOPY_PATH_KEY = "org.apache.mahout.clustering.canopy.output.clusters";
  public static final String T1_KEY = "org.apache.mahout.clustering.canopy.t1";
  public static final String T2_KEY = "org.apache.mahout.clustering.canopy.t2";
  public static final String DISTANCE_MEASURE_KEY = "org.apache.mahout.clustering.canopy.measure";

  // Default paths
  public static final String DEFAULT_CLUSTERED_POINTS_DIRECTORY = "clusteredPoints";
  public static final String DEFAULT_CLUSTERS_DIRECTORY = Cluster.CLUSTERS_DIR + '0';

  // Default configuration
  public static final String DEFAULT_DISTANCE_MEASURE_CLASSNAME = SquaredEuclideanDistanceMeasure.class.getName();
  public static final double DEFAULT_T1 = 3.0;
  public static final double DEFAULT_T2 = 1.5;

  private Configuration configuration;
  private boolean runsClustering;

  public CanopyConfiguration(Configuration configuration, Path input, Path outputBase) {
    setConfiguration(configuration);
    setInputPath(input);
    setOutputBase(outputBase);
    setCanopyPath(new Path(outputBase, DEFAULT_CLUSTERS_DIRECTORY));
    setPointsPath(new Path(outputBase, DEFAULT_CLUSTERED_POINTS_DIRECTORY));

    setRunsClustering(true);
    setDistanceMeasure(new SquaredEuclideanDistanceMeasure());
    setT1(DEFAULT_T1);
    setT2(DEFAULT_T2);
  }

  public void setConfiguration(Configuration configuration) {
    if (configuration == null) {
      this.configuration = new Configuration();
    } else {
      this.configuration = configuration;
    }
  }

  public void setInputPath(Path input) {
    Preconditions.checkArgument(input != null, "Input path cannot be null");
    this.configuration.set(INPUT_KEY, input.toString());
  }

  public Path getInputPath() {
    return new Path(this.configuration.get(INPUT_KEY));
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  private void setOutputBase(Path outputBase) {
    Preconditions.checkArgument(outputBase != null, "Output base path cannot be null");
    this.configuration.set(OUTPUT_BASE_KEY, outputBase.toString());
  }

  public Path getOutputBasePath() {
    return new Path(this.configuration.get(OUTPUT_BASE_KEY));
  }

  public void setDistanceMeasure(DistanceMeasure distanceMeasure) {
    Preconditions.checkArgument(distanceMeasure != null, "Distance measure cannot be null");
    this.configuration.set(DISTANCE_MEASURE_KEY, distanceMeasure.getClass().getName());
  }

  public String getDistanceMeasureClassName() {
    return configuration.get(DISTANCE_MEASURE_KEY);
  }

  public void setT1(double t1) {
    this.configuration.set(T1_KEY, String.valueOf(t1));
  }

  public double getT1() {
    return Double.valueOf(configuration.get(T1_KEY));
  }

  public void setT2(double t2) {
    this.configuration.set(T2_KEY, String.valueOf(t2));
  }

  public double getT2() {
    return Double.valueOf(configuration.get(T2_KEY));
  }

  public void setRunsClustering(boolean runsClustering) {
    this.runsClustering = runsClustering;
  }

  public boolean runsClustering() {
    return this.runsClustering;
  }

  private void setCanopyPath(Path canopyPath) {
    this.configuration.set(CANOPY_PATH_KEY, canopyPath.toString());
  }

  public Path getCanopyPath() {
    return new Path(this.configuration.get(CANOPY_PATH_KEY));
  }

  private void setPointsPath(Path pointsPath) {
    this.configuration.set(POINTS_PATH_KEY, pointsPath.toString());
  }

  public Path getPointsPath() {
    return new Path(this.configuration.get(POINTS_PATH_KEY));
  }
}
