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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.config.SerializableConfiguration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds default, mandatory and optional configuration for the K-Means algorithm. Can be serialized into a Hadoop
 * {@link Configuration} object and deserialized at mappers, combiners and reducers.
 */
public class KMeansConfiguration implements SerializableConfiguration<KMeansConfiguration> {

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

  /**
   * Configuration key for the serialized configuration string
   */
  public static final String SERIALIZATION_KEY = "org.apache.mahout.clustering.kmeans.serialization";

  // Default configuration
  static String DEFAULT_DISTANCE_MEASURE_CLASSNAME = SquaredEuclideanDistanceMeasure.class.getName();
  static double DEFAULT_CONVERGENCE_DELTA = 0.5;

  // Input
  private Path inputVectors;
  private Path inputClusters;

  // Output
  private Path outputClusters;

  // Algorithm config
  private Configuration configuration;
  private int maxIterations;
  private DistanceMeasure distanceMeasure;
  private double convergenceDelta;
  private String distanceMeasureClassName;
  private boolean runClustering;

  // Required for serialization
  public KMeansConfiguration() {
  }

  public KMeansConfiguration(Configuration configuration, Path inputVectors, Path outputClusters, Path inputClusters, int maxIterations) throws IOException {
    Preconditions.checkArgument(configuration != null, "Configuration cannot be null");
    Preconditions.checkArgument(inputVectors != null, "Input path cannot be null");
    Preconditions.checkArgument(outputClusters != null, "Output path cannot be null");
    Preconditions.checkArgument(inputClusters != null, "Cluster path cannot be null");
    Preconditions.checkArgument(maxIterations > 0, "Max iterations must be greater than zero");

    this.configuration = configuration;
    this.inputVectors = inputVectors;
    this.inputClusters = inputClusters;
    this.outputClusters = outputClusters;
    this.maxIterations = maxIterations;

    this.setConvergenceDelta(DEFAULT_CONVERGENCE_DELTA);
    this.runClustering = true;
    this.distanceMeasure = new SquaredEuclideanDistanceMeasure();
    this.setDistanceMeasure(distanceMeasure);
  }

  @Override
  public Configuration serializeInConfiguration() throws IOException {
    String serializedKMeansConfig = new DefaultStringifier<KMeansConfiguration>(configuration, KMeansConfiguration.class).toString(this);

    Configuration serializedConfig = new Configuration(configuration);
    serializedConfig.set(SERIALIZATION_KEY, serializedKMeansConfig);

    return serializedConfig;
  }

  @Override
  public KMeansConfiguration getFromConfiguration(Configuration configuration) throws IOException {
    String serializedConfigString = configuration.get(KMeansConfiguration.SERIALIZATION_KEY);
    return new DefaultStringifier<KMeansConfiguration>(configuration, KMeansConfiguration.class).fromString(serializedConfigString);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(getInputVectors().toString());
    out.writeUTF(getOutputclusters().toString());
    out.writeInt(getMaxIterations());
    out.writeUTF(getInputClusters().toString());
    out.writeDouble(getConvergenceDelta());
    out.writeBoolean(runsClustering());
    out.writeUTF(getDistanceMeasureClassName());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      inputVectors = new Path(in.readUTF());
      outputClusters = new Path(in.readUTF());
      maxIterations = in.readInt();
      inputClusters = new Path(in.readUTF());
      convergenceDelta = in.readDouble();
      runClustering = in.readBoolean();
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      distanceMeasureClassName = in.readUTF();
      distanceMeasure = ccl.loadClass(distanceMeasureClassName).asSubclass(DistanceMeasure.class).newInstance();

    } catch (InstantiationException e) {
      throw new RuntimeException("Could not deserialize " + KMeansConfiguration.class.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not deserialize " + KMeansConfiguration.class.getName(), e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not deserialize " + KMeansConfiguration.class.getName(), e);
    }
  }

  // TODO: Remove since serializeInConfiguration is implemented?
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Returns the input vectors path
   *
   * @return input vectors path
   */
  public Path getInputVectors() {
    return inputVectors;
  }

  /**
   * Returns the input clusters path
   *
   * @return initial clusters path
   */
  public Path getInputClusters() {
    return inputClusters;
  }

  /**
   * Sets the input clusters path
   *
   * @param inputClusters the new input clusters path
   */
  public void setInputClusters(Path inputClusters) {
    this.inputClusters = inputClusters;
  }

  /**
   * Returns the output clusters path
   *
   * @return output clusters path
   */
  public Path getOutputclusters() {
    return outputClusters;
  }

  /**
   * Sets the centroid output path
   *
   * @param outputClusters the new centroid output path
   */
  public void setOutputClusters(Path outputClusters) {
    this.outputClusters = outputClusters;
  }

  /**
   * Returns the output points path
   *
   * @return output points path
   */
  public Path getOutputPoints() {
    return new Path(getOutputclusters().getParent(), AbstractCluster.CLUSTERED_POINTS_DIR);
  }

  public void setDistanceMeasure(DistanceMeasure distanceMeasure) {
    this.distanceMeasure = distanceMeasure;
    setDistanceMeasureClassName(distanceMeasure.getClass().getName());
  }

  private void setDistanceMeasureClassName(String className) {
    this.distanceMeasureClassName = className;
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
    return distanceMeasureClassName;
  }

  public void setConvergenceDelta(double convergenceDelta) {
    this.convergenceDelta = convergenceDelta;
  }

  public double getConvergenceDelta() {
    return convergenceDelta;
  }

  public void setMaxIterations(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  public int getMaxIterations() {
    return maxIterations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KMeansConfiguration that = (KMeansConfiguration) o;

    if (Double.compare(that.convergenceDelta, convergenceDelta) != 0) return false;
    if (maxIterations != that.maxIterations) return false;
    if (runClustering != that.runClustering) return false;
    if (inputClusters != null ? !inputClusters.equals(that.inputClusters) : that.inputClusters != null) return false;
    if (distanceMeasure != null ? !distanceMeasure.getClass().equals(that.distanceMeasure.getClass()) : that.distanceMeasure != null)
      return false;
    if (distanceMeasureClassName != null ? !distanceMeasureClassName.equals(that.distanceMeasureClassName) : that.distanceMeasureClassName != null)
      return false;
    if (inputVectors != null ? !inputVectors.equals(that.inputVectors) : that.inputVectors != null) return false;
    if (outputClusters != null ? !outputClusters.equals(that.outputClusters) : that.outputClusters != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = inputVectors != null ? inputVectors.hashCode() : 0;
    result = 31 * result + (outputClusters != null ? outputClusters.hashCode() : 0);
    result = 31 * result + (inputClusters != null ? inputClusters.hashCode() : 0);
    result = 31 * result + maxIterations;
    result = 31 * result + (distanceMeasure != null ? distanceMeasure.hashCode() : 0);
    temp = convergenceDelta != +0.0d ? Double.doubleToLongBits(convergenceDelta) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + (distanceMeasureClassName != null ? distanceMeasureClassName.hashCode() : 0);
    result = 31 * result + (runClustering ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "KMeansConfiguration{" +
        "inputVectors=" + inputVectors +
        ", inputClusters=" + inputClusters +
        ", outputClusters=" + outputClusters +
        ", outputPoints=" + getOutputPoints() +
        ", maxIterations=" + maxIterations +
        ", distanceMeasure=" + distanceMeasure +
        ", convergenceDelta=" + convergenceDelta +
        ", distanceMeasureClassName='" + distanceMeasureClassName + '\'' +
        ", runClustering=" + runClustering +
        '}';
  }
}
