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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * MapReduce implementation of the of the canopy clustering algorithm.
 */
public class CanopyMapReduceAlgorithm {

  /**
   * Runs the MapReduced canopy clustering algorihtm with the given {@link CanopyConfiguration}. Optionally creates
   * points if the runsClustering flag is set on the {@link CanopyConfiguration}
   *
   * @param canopyConfiguration configuration for the canopy algorithm
   * @return canopy configuration
   * @throws IOException if paths cannot be found
   * @throws ClassNotFoundException if distance measure class could not be found
   * @throws InterruptedException if Hadoop job was interrupted
   */
  public CanopyConfiguration run(CanopyConfiguration canopyConfiguration) throws IOException, ClassNotFoundException, InterruptedException {

    canopyConfiguration = runCanopyJob(canopyConfiguration);

    if (canopyConfiguration.runsClustering()) {
      runCanopyPointsJob(canopyConfiguration);
    }

    return canopyConfiguration;
  }

  private CanopyConfiguration runCanopyJob(CanopyConfiguration canopyConfiguration) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = canopyConfiguration.getConfiguration();

    Job job = new Job(configuration, "Canopy Driver running buildClusters over input: " + canopyConfiguration.getInputPath());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(CanopyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setReducerClass(CanopyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Canopy.class);
    job.setNumReduceTasks(1);
    job.setJarByClass(CanopyDriver.class);

    FileInputFormat.addInputPath(job, canopyConfiguration.getInputPath());
    FileOutputFormat.setOutputPath(job, canopyConfiguration.getCanopyPath());

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("Canopy Job failed processing " + canopyConfiguration.getInputPath().toString());
    }

    return canopyConfiguration;
  }

  private void runCanopyPointsJob(CanopyConfiguration canopyConfiguration) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration configuration = canopyConfiguration.getConfiguration();

    Job job = new Job(configuration, "Canopy Driver running clusterData over input: " + canopyConfiguration.getInputPath());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(ClusterMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(WeightedVectorWritable.class);
    job.setNumReduceTasks(0);
    job.setJarByClass(CanopyDriver.class);

    FileInputFormat.addInputPath(job, canopyConfiguration.getInputPath());
    FileOutputFormat.setOutputPath(job, canopyConfiguration.getPointsPath());

    HadoopUtil.overwriteOutput(canopyConfiguration.getPointsPath());

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("Canopy Clustering failed processing " + canopyConfiguration.getPointsPath().toString());
    }
  }
}

