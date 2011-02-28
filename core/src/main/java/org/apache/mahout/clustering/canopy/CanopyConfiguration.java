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
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.config.SerializableConfiguration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds all the configuration for the {@link CanopyMapReduceAlgorithm} and {@link CanopySequentialAlgorithm}
 */
public class CanopyConfiguration implements SerializableConfiguration<CanopyConfiguration> {

  /**
   * Configuration key for the serialized configuration string
   */
  private static final String SERIALIZATION_KEY = "org.apache.mahout.clustering.kmeans.serialization";

  // Hadoop configuration keys
  public static final String DISTANCE_MEASURE_KEY = "org.apache.mahout.clustering.canopy.measure";
  public static final String T1_KEY = "org.apache.mahout.clustering.canopy.t1";
  public static final String T2_KEY = "org.apache.mahout.clustering.canopy.t2";
  public static final String T3_KEY = "org.apache.mahout.clustering.canopy.t3";
  public static final String T4_KEY = "org.apache.mahout.clustering.canopy.t4";

  // Default paths
  public static final String DEFAULT_CLUSTERED_POINTS_DIRECTORY = "clusteredPoints";
  public static final String DEFAULT_CLUSTERS_DIRECTORY = Cluster.CLUSTERS_DIR + '0';

  // Default configuration
  public static final String DEFAULT_DISTANCE_MEASURE_CLASSNAME = SquaredEuclideanDistanceMeasure.class.getName();
  public static final double DEFAULT_T1 = 3.0;
  public static final double DEFAULT_T2 = 1.5;

  private Configuration configuration;

  private Path inputVectorPath;
  private Path baseOutputPath;
  private Path canopyOutputPath;
  private Path pointsOutputPath;
  private String distanceMeasureClassName;
  private DistanceMeasure distanceMeasure;
  private boolean runsClustering;
  private double t1;
  private double t2;
  private double t3;
  private double t4;

  public CanopyConfiguration(Configuration configuration, Path input, Path outputBase) {
    setConfiguration(configuration);
    setInputPath(input);
    setOutputBase(outputBase);
    setCanopyOutputPath(new Path(outputBase, DEFAULT_CLUSTERS_DIRECTORY));
    setPointsOutputPath(new Path(outputBase, DEFAULT_CLUSTERED_POINTS_DIRECTORY));

    setRunsClustering(true);
    setDistanceMeasure(new SquaredEuclideanDistanceMeasure());
    setT1(DEFAULT_T1);
    setT2(DEFAULT_T2);
  }

  // Required for serialization
  protected CanopyConfiguration() {
  }

  @Override
  public CanopyConfiguration getFromConfiguration(Configuration configuration) throws IOException {
    String serializedConfigString = configuration.get(CanopyConfiguration.SERIALIZATION_KEY);
    return new DefaultStringifier<CanopyConfiguration>(configuration, CanopyConfiguration.class).fromString(serializedConfigString);
  }

  @Override
  public Configuration serializeInConfiguration() throws IOException {
    String serializedCanopyConfig = new DefaultStringifier<CanopyConfiguration>(configuration, CanopyConfiguration.class).toString(this);

    Configuration serializedConfig = new Configuration(configuration);
    serializedConfig.set(SERIALIZATION_KEY, serializedCanopyConfig);

    return serializedConfig;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(inputVectorPath.toString());
    dataOutput.writeUTF(baseOutputPath.toString());
    dataOutput.writeUTF(canopyOutputPath.toString());
    dataOutput.writeUTF(pointsOutputPath.toString());
    dataOutput.writeBoolean(runsClustering);
    dataOutput.writeDouble(t1);
    dataOutput.writeDouble(t2);
    dataOutput.writeDouble(t3);
    dataOutput.writeDouble(t4);
    dataOutput.writeUTF(distanceMeasureClassName);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    try {
      inputVectorPath = new Path(dataInput.readUTF());
      baseOutputPath = new Path(dataInput.readUTF());
      canopyOutputPath = new Path(dataInput.readUTF());
      pointsOutputPath = new Path(dataInput.readUTF());
      runsClustering = dataInput.readBoolean();
      t1 = dataInput.readDouble();
      t2 = dataInput.readDouble();
      t3 = dataInput.readDouble();
      t4 = dataInput.readDouble();
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      distanceMeasureClassName = dataInput.readUTF();
      distanceMeasure = ccl.loadClass(distanceMeasureClassName).asSubclass(DistanceMeasure.class).newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not deserialize " + CanopyConfiguration.class.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not deserialize " + CanopyConfiguration.class.getName(), e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not deserialize " + CanopyConfiguration.class.getName(), e);
    }
  }

  public void setConfiguration(Configuration configuration) {
    if (configuration == null) {
      this.configuration = new Configuration();
    } else {
      this.configuration = configuration;
    }
  }

  public void setInputPath(Path inputVectorPath) {
    Preconditions.checkNotNull(inputVectorPath, "Input path cannot be null");
    this.inputVectorPath = inputVectorPath;
  }

  public Path getInputPath() {
    return inputVectorPath;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  private void setOutputBase(Path outputBasePath) {
    Preconditions.checkNotNull(outputBasePath, "Output base path cannot be null");
    this.baseOutputPath = outputBasePath;
  }

  public Path getBaseOutputPath() {
    return baseOutputPath;
  }

  public void setDistanceMeasure(DistanceMeasure distanceMeasure) {
    Preconditions.checkNotNull(distanceMeasure, "Distance measure cannot be null");
    this.distanceMeasure = distanceMeasure;
    setDistanceMeasureClassName(distanceMeasure.getClass().getName());
  }

  public DistanceMeasure getDistanceMeasure() {
    return distanceMeasure;
  }

  public void setDistanceMeasureClassName(String distanceMeasureClassName) {
    this.distanceMeasureClassName = distanceMeasureClassName;
  }

  public String getDistanceMeasureClassName() {
    return this.distanceMeasureClassName;
  }

  public void setT1(double t1) {
    this.t1 = t1;
  }

  public double getT1() {
    return t1;
  }

  public void setT2(double t2) {
    this.t2 = t2;
  }

  public double getT2() {
    return t2;
  }

  public void setT3(double t3) {
    this.t3 = t3;
  }

  public Double getT3() {
    return t3;
  }

  public void setT4(double t4) {
    this.t4 = t4;
  }

  public Double getT4() {
    return t4;
  }

  public void setRunsClustering(boolean runsClustering) {
    this.runsClustering = runsClustering;
  }

  public boolean runsClustering() {
    return this.runsClustering;
  }

  private void setCanopyOutputPath(Path canopyOutputPath) {
    this.canopyOutputPath = canopyOutputPath;
  }

  public Path getCanopyOutputPath() {
    return canopyOutputPath;
  }

  private void setPointsOutputPath(Path pointsOutputPath) {
    this.pointsOutputPath = pointsOutputPath;
  }

  public Path getPointsOutputPath() {
    return pointsOutputPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CanopyConfiguration that = (CanopyConfiguration) o;

    if (runsClustering != that.runsClustering) return false;
    if (Double.compare(that.t1, t1) != 0) return false;
    if (Double.compare(that.t2, t2) != 0) return false;
    if (Double.compare(that.t3, t3) != 0) return false;
    if (Double.compare(that.t4, t4) != 0) return false;
    if (canopyOutputPath != null ? !canopyOutputPath.equals(that.canopyOutputPath) : that.canopyOutputPath != null)
      return false;
    if (distanceMeasure != null ? !distanceMeasure.getClass().equals(that.distanceMeasure.getClass()) : that.distanceMeasure != null)
      return false;
    if (distanceMeasureClassName != null ? !distanceMeasureClassName.equals(that.distanceMeasureClassName) : that.distanceMeasureClassName != null)
      return false;
    if (inputVectorPath != null ? !inputVectorPath.equals(that.inputVectorPath) : that.inputVectorPath != null)
      return false;
    if (baseOutputPath != null ? !baseOutputPath.equals(that.baseOutputPath) : that.baseOutputPath != null)
      return false;
    if (pointsOutputPath != null ? !pointsOutputPath.equals(that.pointsOutputPath) : that.pointsOutputPath != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = inputVectorPath != null ? inputVectorPath.hashCode() : 0;
    result = 31 * result + (baseOutputPath != null ? baseOutputPath.hashCode() : 0);
    result = 31 * result + (canopyOutputPath != null ? canopyOutputPath.hashCode() : 0);
    result = 31 * result + (pointsOutputPath != null ? pointsOutputPath.hashCode() : 0);
    result = 31 * result + (distanceMeasureClassName != null ? distanceMeasureClassName.hashCode() : 0);
    result = 31 * result + (distanceMeasure != null ? distanceMeasure.hashCode() : 0);
    result = 31 * result + (runsClustering ? 1 : 0);
    temp = t1 != +0.0d ? Double.doubleToLongBits(t1) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = t2 != +0.0d ? Double.doubleToLongBits(t2) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = t3 != +0.0d ? Double.doubleToLongBits(t3) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = t4 != +0.0d ? Double.doubleToLongBits(t4) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "CanopyConfiguration{" +
            "inputVectorPath=" + inputVectorPath +
            ", outputBasePath=" + baseOutputPath +
            ", canopyOutputPath=" + canopyOutputPath +
            ", pointsOutputPath=" + pointsOutputPath +
            ", distanceMeasure=" + distanceMeasure +
            ", distanceMeasureClassName='" + distanceMeasureClassName + '\'' +
            ", runsClustering=" + runsClustering +
            ", t1=" + t1 +
            ", t2=" + t2 +
            ", t3=" + t3 +
            ", t4=" + t4 +
            '}';
  }
}
