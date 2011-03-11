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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanopyDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(CanopyDriver.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CanopyDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.distanceMeasureOption().create());
    addOption(DefaultOptionCreator.t1Option().create());
    addOption(DefaultOptionCreator.t2Option().create());
    addOption(DefaultOptionCreator.overwriteOption().create());
    addOption(DefaultOptionCreator.clusteringOption().create());
    addOption(DefaultOptionCreator.methodOption().create());

    Map<String, String> argMap = parseArguments(args);
    if (argMap == null) {
      return -1;
    }

    Path input = getInputPath();
    Path output = getOutputPath();
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.overwriteOutput(output);
    }
    String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
    double t1 = Double.parseDouble(getOption(DefaultOptionCreator.T1_OPTION));
    double t2 = Double.parseDouble(getOption(DefaultOptionCreator.T2_OPTION));
    boolean runClustering = hasOption(DefaultOptionCreator.CLUSTERING_OPTION);
    boolean runSequential = getOption(DefaultOptionCreator.METHOD_OPTION).equalsIgnoreCase(
        DefaultOptionCreator.SEQUENTIAL_METHOD);
    ClassLoader ccl = Thread.currentThread().getContextClassLoader();
    DistanceMeasure measure = ccl.loadClass(measureClass).asSubclass(DistanceMeasure.class).newInstance();

    run(getConf(), input, output, measure, t1, t2, runClustering, runSequential);
    return 0;
  }

  /**
   * Convenience method creates new Configuration()
   * Build a directory of Canopy clusters from the input arguments and, if requested,
   * cluster the input vectors using these clusters
   *
   * @param input         the Path to the directory containing input vectors
   * @param output        the Path for all output directories
   * @param t1            the double T1 distance metric
   * @param t2            the double T2 distance metric
   * @param runClustering cluster the input vectors if true
   * @param runSequential execute sequentially if true
   */
  public static void run(Path input,
                         Path output,
                         DistanceMeasure measure,
                         double t1,
                         double t2,
                         boolean runClustering,
                         boolean runSequential) throws IOException, InterruptedException, ClassNotFoundException,
    InstantiationException, IllegalAccessException {


    run(new Configuration(), input, output, measure, t1, t2, runClustering, runSequential);
  }

  /**
   * Build a directory of Canopy clusters from the input arguments and, if requested,
   * cluster the input vectors using these clusters
   *
   * @param input         the Path to the directory containing input vectors
   * @param output        the Path for all output directories
   * @param t1            the double T1 distance metric
   * @param t2            the double T2 distance metric
   * @param runClustering cluster the input vectors if true
   * @param runSequential execute sequentially if true
   */
  public static void run(Configuration conf,
                         Path input,
                         Path output,
                         DistanceMeasure measure,
                         double t1,
                         double t2,
                         boolean runClustering,
                         boolean runSequential) throws IOException, InterruptedException, ClassNotFoundException,
    InstantiationException, IllegalAccessException {

    log.info("Build Clusters Input: {} Out: {} " + "Measure: {} t1: {} t2: {}",
      new Object[]{input, output, measure, t1, t2});

    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(conf, input, output);
    canopyConfiguration.setT1(t1);
    canopyConfiguration.setT2(t2);
    canopyConfiguration.setDistanceMeasure(measure);
    canopyConfiguration.setRunsClustering(runClustering);

    if (runSequential) {
      new CanopySequentialAlgorithm().run(canopyConfiguration);
    } else {
      new CanopyMapReduceAlgorithm().run(canopyConfiguration);
    }
  }
}
