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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;

/**
 * Holds default, mandatory and optional configuration for the K-Means algorithm.
 */
public class KMeansConfiguration {

  // Hadoop configuration keys

  /**
   * Configuration key for distance measure to use.
   */
  public static String DISTANCE_MEASURE_KEY = "org.apache.mahout.clustering.kmeans.measure";
  /**
   * Configuration key for convergence threshold.
   */
  public static String CLUSTER_CONVERGENCE_KEY = "org.apache.mahout.clustering.kmeans.convergence";
  /**
   * Configuration key for iteration cluster path
   */
  public static String CLUSTER_PATH_KEY = "org.apache.mahout.clustering.kmeans.path";

  // Default configuration
  static String DEFAULT_DISTANCE_MEASURE_CLASSNAME = SquaredEuclideanDistanceMeasure.class.getName();
  static double DEFAULT_CONVERGENCE_DELTA = 0.5;

  // Mandatory configuration
  private Configuration configuration;
  private Path input;
  private Path output;
  private int maxIterations;
  private DistanceMeasure distanceMeasure;
  private boolean runClustering;

  public KMeansConfiguration(Configuration configuration, Path input, Path output, Path clusterPath, int maxIterations) {
    Preconditions.checkArgument(configuration != null, "Configuration cannot be null");
    Preconditions.checkArgument(input != null, "Input path cannot be null");
    Preconditions.checkArgument(output != null, "Output path cannot be null");
    Preconditions.checkArgument(clusterPath != null, "Cluster path cannot be null");
    Preconditions.checkArgument(maxIterations > 0, "Max iterations must be greater than zero");

    this.configuration = configuration;
    this.input = input;
    this.output = output;
    this.maxIterations = maxIterations;

    this.setClusterPath(clusterPath);
    this.setConvergenceDelta(DEFAULT_CONVERGENCE_DELTA);
    this.runClustering = true;
    this.distanceMeasure = new SquaredEuclideanDistanceMeasure();
    this.setDistanceMeasure(distanceMeasure);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getInput() {
    return input;
  }

  public Path getOutput() {
    return output;
  }

  public void setDistanceMeasure(DistanceMeasure distanceMeasure) {
    this.distanceMeasure = distanceMeasure;
    this.configuration.set(DISTANCE_MEASURE_KEY, this.distanceMeasure.getClass().getName());
  }

  public DistanceMeasure getDistanceMeasure() {
    return distanceMeasure;
  }

  public boolean runsClustering() {
    return runClustering;
  }

  public void setRunClustering(boolean runClustering) {
    this.runClustering = runClustering;
  }

  public String getDistanceMeasureClassName() {
    return this.configuration.get(DISTANCE_MEASURE_KEY);
  }

  public void setConvergenceDelta(double convergenceDelta) {
    this.configuration.set(CLUSTER_CONVERGENCE_KEY, String.valueOf(convergenceDelta));
  }

  public double getConvergenceDelta() {
    return Double.valueOf(configuration.get(CLUSTER_CONVERGENCE_KEY));
  }

  public void setMaxIterations(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  public int getMaxIterations() {
    return maxIterations;
  }

  public void setOutput(Path output) {
    this.output = output;
  }

  public void setClusterPath(Path clusterPath) {
    this.configuration.set(CLUSTER_PATH_KEY, clusterPath.toString());
  }

  public Path getClusterPath() {
    return new Path(configuration.get(CLUSTER_PATH_KEY));
  }

  public Path getPointsOutput() {
    return new Path(getOutput().getParent(), AbstractCluster.CLUSTERED_POINTS_DIR);
  }
}
