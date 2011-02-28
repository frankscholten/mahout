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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.ClusterObservations;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MapReduce implementation of the K-Means algorithm for creating clusters and points.
 */
public class KMeansMapReduceAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(KMeansMapReduceAlgorithm.class);

  /**
   * Runs the K-Means algorithm with the given options
   *
   * @param kMeansConfiguration configuration for K-Means
   * @return the K-Means configuration with updated output paths
   * @throws IOException            if paths cannot be found
   * @throws ClassNotFoundException if distance measure class can not be found
   * @throws InterruptedException   if job gets interrupted
   */
  public KMeansConfiguration run(KMeansConfiguration kMeansConfiguration) throws IOException, ClassNotFoundException, InterruptedException {
    boolean converged = false;
    int iteration = 1;

    Path outputBaseDir = kMeansConfiguration.getOutput();

    while (!converged && (iteration <= kMeansConfiguration.getMaxIterations())) {
      log.info("K-Means Iteration {}", iteration);
      // point the output to a new directory per iteration
      Path clustersOut = new Path(outputBaseDir, AbstractCluster.CLUSTERS_DIR + iteration);

      kMeansConfiguration.setOutput(clustersOut);

      converged = isConverged(kMeansConfiguration);
      // now point the input to the old output directory
      kMeansConfiguration.setClusterPath(clustersOut);

      iteration++;
    }

    if (kMeansConfiguration.runsClustering()) {
      Job job = createKMeansPointsJob(kMeansConfiguration);

      if (!job.waitForCompletion(true)) {
        throw new InterruptedException("K-Means points failed processing " + kMeansConfiguration.getClusterPath().toString());
      }
    }

    return kMeansConfiguration;
  }

  private boolean isConverged(KMeansConfiguration kMeansConfiguration) throws IOException, ClassNotFoundException, InterruptedException {
    HadoopUtil.overwriteOutput(kMeansConfiguration.getOutput());
    Configuration configuration = kMeansConfiguration.getConfiguration();

    Job job = createKMeansIterationJob(kMeansConfiguration);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("K-Means Iteration failed processing " + kMeansConfiguration.getInput().toString());
    }
    FileSystem fs = FileSystem.get(kMeansConfiguration.getOutput().toUri(), configuration);

    FileStatus[] parts = fs.listStatus(kMeansConfiguration.getOutput());
    for (FileStatus part : parts) {
      String name = part.getPath().getName();
      if (name.startsWith("part") && !name.endsWith(".crc")) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, part.getPath(), configuration);
        try {
          Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
          Cluster value = new Cluster();
          while (reader.next(key, value)) {
            if (!value.isConverged()) {
              return false;
            }
          }
        } catch (InstantiationException e) { // shouldn't happen
          log.error("Exception", e);
          throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
          log.error("Exception", e);
          throw new IllegalStateException(e);
        } finally {
          reader.close();
        }
      }
    }
    return true;
  }

  private Job createKMeansIterationJob(KMeansConfiguration kmeansConfiguration) throws IOException {
    Job job = new Job(kmeansConfiguration.getConfiguration(), "KMeans iteration using clustersPath: " + kmeansConfiguration.getClusterPath());
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ClusterObservations.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Cluster.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(KMeansMapper.class);
    job.setCombinerClass(KMeansCombiner.class);
    job.setReducerClass(KMeansReducer.class);

    FileInputFormat.addInputPath(job, kmeansConfiguration.getInput());
    FileOutputFormat.setOutputPath(job, kmeansConfiguration.getOutput());

    job.setJarByClass(KMeansDriver.class);

    return job;
  }

  private Job createKMeansPointsJob(KMeansConfiguration kMeansConfiguration) throws IOException {
    log.info("Clustering data");

    if (log.isInfoEnabled()) {
      log.info("Running Clustering");
      log.info("Input: {} Clusters In: {} Out: {} Distance: {}", new Object[]{kMeansConfiguration.getInput(), kMeansConfiguration.getClusterPath(), kMeansConfiguration.getOutput(), kMeansConfiguration.getDistanceMeasure()
      });
      log.info("convergence: {} Input Vectors: {}", kMeansConfiguration.getConvergenceDelta(), VectorWritable.class.getName());
    }

    Job job = new Job(kMeansConfiguration.getConfiguration(), "KMeans Driver points job over input: " + kMeansConfiguration.getInput());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(WeightedVectorWritable.class);

    FileInputFormat.setInputPaths(job, kMeansConfiguration.getInput());
    HadoopUtil.overwriteOutput(kMeansConfiguration.getPointsOutput());
    FileOutputFormat.setOutputPath(job, kMeansConfiguration.getPointsOutput());

    job.setMapperClass(KMeansClusterMapper.class);
    job.setNumReduceTasks(0);
    job.setJarByClass(KMeansDriver.class);

    return job;
  }
}
