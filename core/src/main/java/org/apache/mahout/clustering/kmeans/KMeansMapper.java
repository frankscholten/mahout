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
package org.apache.mahout.clustering.kmeans;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.ClusterObservations;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class KMeansMapper extends Mapper<WritableComparable<?>, VectorWritable, Text, ClusterObservations> {

  private KMeansClusterer clusterer;

  private final Collection<Cluster> clusters = new ArrayList<Cluster>();

  @Override
  protected void map(WritableComparable<?> key, VectorWritable point, Context context)
          throws IOException, InterruptedException {
    this.clusterer.emitPointToNearestCluster(point.get(), this.clusters, context);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    KMeansConfiguration kMeansConfiguration = KMeansConfiguration.deserialized(context.getConfiguration());

    DistanceMeasure measure = kMeansConfiguration.getDistanceMeasure();
    measure.configure(context.getConfiguration());

    this.clusterer = new KMeansClusterer(measure);

    Path clusterPath = kMeansConfiguration.getInputClusters();
    if ((clusterPath != null) && (clusterPath.toString().length() > 0)) {
      KMeansUtil.configureWithClusterInfo(clusterPath, clusters);
      if (clusters.isEmpty()) {
        throw new IllegalStateException("No clusters found. Check your -c path.");
      }
    }
  }

  /**
   * Configure the mapper by providing its clusters. Used by unit tests.
   *
   * @param clusters a List<Cluster>
   * @param measure  TODO
   */
  void setup(Collection<Cluster> clusters, DistanceMeasure measure) {
    this.clusters.clear();
    this.clusters.addAll(clusters);
    this.clusterer = new KMeansClusterer(measure);
  }
}
