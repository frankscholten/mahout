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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(KMeansDriver.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new KMeansDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.distanceMeasureOption().create());
    addOption(DefaultOptionCreator.clustersInOption()
        .withDescription("The input centroids, as Vectors.  Must be a SequenceFile of Writable, Cluster/Canopy.  "
            + "If k is also specified, then a random set of vectors will be selected"
            + " and written out to this path first")
        .create());
    addOption(DefaultOptionCreator.numClustersOption()
        .withDescription("The k in k-Means.  If specified, then a random selection of k Vectors will be chosen"
            + " as the Centroid and written to the clusters input path.").create());
    addOption(DefaultOptionCreator.convergenceOption().create());
    addOption(DefaultOptionCreator.maxIterationsOption().create());
    addOption(DefaultOptionCreator.overwriteOption().create());
    addOption(DefaultOptionCreator.clusteringOption().create());
    addOption(DefaultOptionCreator.methodOption().create());

    if (parseArguments(args) == null) {
      return -1;
    }

    Path input = getInputPath();
    Path clusters = new Path(getOption(DefaultOptionCreator.CLUSTERS_IN_OPTION));
    Path output = getOutputPath();
    String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
    if (measureClass == null) {
      measureClass = SquaredEuclideanDistanceMeasure.class.getName();
    }
    double convergenceDelta = Double.parseDouble(getOption(DefaultOptionCreator.CONVERGENCE_DELTA_OPTION));
    int maxIterations = Integer.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.overwriteOutput(output);
    }
    ClassLoader ccl = Thread.currentThread().getContextClassLoader();
    DistanceMeasure measure = ccl.loadClass(measureClass).asSubclass(DistanceMeasure.class).newInstance();

    if (hasOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION)) {
      clusters = RandomSeedGenerator.buildRandom(input, clusters, Integer
          .parseInt(getOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION)), measure);
    }
    boolean runClustering = hasOption(DefaultOptionCreator.CLUSTERING_OPTION);
    boolean runSequential = getOption(DefaultOptionCreator.METHOD_OPTION).equalsIgnoreCase(
        DefaultOptionCreator.SEQUENTIAL_METHOD);
    run(getConf(), input, clusters, output, measure, convergenceDelta, maxIterations, runClustering, runSequential);
    return 0;
  }

  /**
   * Iterate over the input vectors to produce clusters and, if requested, use the
   * results of the final iteration to cluster the input vectors.
   *
   * @param input            the directory pathname for input points
   * @param clustersIn       the directory pathname for initial & computed clusters
   * @param output           the directory pathname for output points
   * @param measure          the DistanceMeasure to use
   * @param convergenceDelta the convergence delta value
   * @param maxIterations    the maximum number of iterations
   * @param runClustering    true if points are to be clustered after iterations are completed
   * @param runSequential    if true execute sequential algorithm
   */
  public static void run(Path input,
                         Path clustersIn,
                         Path output,
                         DistanceMeasure measure,
                         double convergenceDelta,
                         int maxIterations,
                         boolean runClustering,
                         boolean runSequential)
      throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    run(new Configuration(),
        input,
        clustersIn,
        output,
        measure,
        convergenceDelta,
        maxIterations,
        runClustering,
        runSequential);
  }

  /**
   * Iterate over the input vectors to produce clusters and, if requested, use the
   * results of the final iteration to cluster the input vectors.
   *
   * @param input            the directory pathname for input points
   * @param clustersIn       the directory pathname for initial & computed clusters
   * @param output           the directory pathname for output points
   * @param measure          the DistanceMeasure to use
   * @param convergenceDelta the convergence delta value
   * @param maxIterations    the maximum number of iterations
   * @param runClustering    true if points are to be clustered after iterations are completed
   * @param runSequential    if true execute sequential algorithm
   */
  public static void run(Configuration conf,
                         Path input,
                         Path clustersIn,
                         Path output,
                         DistanceMeasure measure,
                         double convergenceDelta,
                         int maxIterations,
                         boolean runClustering,
                         boolean runSequential)
      throws IOException, InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {

    // iterate until the clusters converge
    if (log.isInfoEnabled()) {
      log.info("Input: {} Clusters In: {} Out: {} Distance: {}", new Object[]{input, clustersIn, output,
          measure.getClass().getName()});
      log.info("convergence: {} max Iterations: {} num Reduce Tasks: {} Input Vectors: {}",
          new Object[]{convergenceDelta, maxIterations, VectorWritable.class.getName()});
    }

    KMeansConfiguration kMeansCentroidsConfig = new KMeansConfiguration(conf, input, output, clustersIn, maxIterations);
    kMeansCentroidsConfig.setRunClustering(runClustering);
    kMeansCentroidsConfig.setDistanceMeasure(measure);
    kMeansCentroidsConfig.setConvergenceDelta(convergenceDelta);

    if (runSequential) {
      KMeansSequentialAlgorithm kMeansSequentialAlgorithm = new KMeansSequentialAlgorithm();
      kMeansSequentialAlgorithm.run(kMeansCentroidsConfig);
    } else {
      KMeansMapReduceAlgorithm kMeansMapReduceAlgorithm = new KMeansMapReduceAlgorithm();
      kMeansMapReduceAlgorithm.run(kMeansCentroidsConfig);
    }
  }
}
