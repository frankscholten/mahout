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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * The {@link org.apache.mahout.clustering.kmeans.KMeansClusterMapper} is responsible for calculating which points belong
 * to which clusters and outputting the information.  This is an optional step, as some applications only care about
 * what the Centroids are.
 *
 * @see KMeansDriver for more information on how to invoke this process
 */
public class KMeansClusterMapper extends Mapper<WritableComparable<?>, VectorWritable, IntWritable, WeightedVectorWritable> {

  private final Collection<Cluster> clusters = new ArrayList<Cluster>();
  private KMeansClusterer clusterer;

  @Override
  protected void map(WritableComparable<?> key, VectorWritable point, Context context) throws IOException, InterruptedException {
    clusterer.outputPointWithClusterInfo(point.get(), clusters, context);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();

    KMeansConfiguration kMeansConfiguration = KMeansConfiguration.deserialized(context.getConfiguration());

    DistanceMeasure measure = kMeansConfiguration.getDistanceMeasure();
    measure.configure(conf);

    Path clusterPath = kMeansConfiguration.getInputClusters();
    if (clusterPath != null && clusterPath.toString().length() != 0) {
      KMeansUtil.configureWithClusterInfo(kMeansConfiguration.getInputClusters(), clusters);
      if (clusters.isEmpty()) {
        throw new IllegalStateException("No clusters found. Check your -c path.");
      }
    }
    this.clusterer = new KMeansClusterer(measure);
  }
}
