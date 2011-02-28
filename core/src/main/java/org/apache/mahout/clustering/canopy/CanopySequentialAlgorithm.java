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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Sequential implementation of the canopy clustering algorithm.
 */
public class CanopySequentialAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(CanopySequentialAlgorithm.class);

  /**
   * Runs the canopy algorithm with the given {@link CanopyConfiguration}
   *
   * @param canopyConfiguration with parameters
   * @return the canopy configuration
   * @throws ClassNotFoundException if distance measure class cannot be found
   * @throws IllegalAccessException if distance measure class cannot be accessed
   * @throws InstantiationException if distance measure class cannot be instantiated
   * @throws IOException            if input or output paths cannot be accessed
   */
  public CanopyConfiguration run(CanopyConfiguration canopyConfiguration) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
    CanopyClusterer clusterer = new CanopyClusterer(canopyConfiguration);
    Collection<Canopy> canopies = new ArrayList<Canopy>();

    FileSystem fs = FileSystem.get(canopyConfiguration.getInputPath().toUri(), canopyConfiguration.getConfiguration());
    FileStatus[] status = fs.listStatus(canopyConfiguration.getInputPath(), new OutputLogFilter());
    for (FileStatus s : status) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, s.getPath(), canopyConfiguration.getConfiguration());
      try {
        Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
        VectorWritable vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
        while (reader.next(key, vw)) {
          clusterer.addPointToCanopies(vw.get(), canopies);
          vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
        }
      } finally {
        reader.close();
      }
    }

    Path path = new Path(canopyConfiguration.getCanopyOutputPath(), "part-r-00000");

    SequenceFile.Writer writer = new SequenceFile.Writer(fs, canopyConfiguration.getConfiguration(), path, Text.class, Canopy.class);
    try {
      for (Canopy canopy : canopies) {
        canopy.computeParameters();
        log.debug("Writing Canopy:" + canopy.getIdentifier() + " center:" + AbstractCluster.formatVector(canopy.getCenter(), null)
          + " numPoints:" + canopy.getNumPoints() + " radius:" + AbstractCluster.formatVector(canopy.getRadius(), null));
        writer.append(new Text(canopy.getIdentifier()), canopy);
      }
    } finally {
      writer.close();
    }

    Collection<Canopy> clusters = new ArrayList<Canopy>();

    fs = FileSystem.get(canopyConfiguration.getCanopyOutputPath().toUri(), canopyConfiguration.getConfiguration());
    status = fs.listStatus(canopyConfiguration.getCanopyOutputPath(), new OutputLogFilter());

    for (FileStatus s : status) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, s.getPath(), canopyConfiguration.getConfiguration());
      try {
        Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
        Canopy value = reader.getValueClass().asSubclass(Canopy.class).newInstance();
        while (reader.next(key, value)) {
          clusters.add(value);
          value = reader.getValueClass().asSubclass(Canopy.class).newInstance();
        }
      } finally {
        reader.close();
      }
    }

    if (canopyConfiguration.runsClustering()) {
      // iterate over all points, assigning each to the closest canopy and outputing that clustering
      fs = FileSystem.get(canopyConfiguration.getInputPath().toUri(), canopyConfiguration.getConfiguration());
      status = fs.listStatus(canopyConfiguration.getInputPath(), new OutputLogFilter());

      int part = 0;
      for (FileStatus s : status) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, s.getPath(), canopyConfiguration.getConfiguration());
        writer = new SequenceFile.Writer(fs, canopyConfiguration.getConfiguration(), new Path(canopyConfiguration.getPointsOutputPath(), "part-m-" + part++), IntWritable.class, WeightedVectorWritable.class);
        try {
          Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
          VectorWritable vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
          while (reader.next(key, vw)) {
            Canopy closest = clusterer.findClosestCanopy(vw.get(), clusters);
            writer.append(new IntWritable(closest.getId()), new WeightedVectorWritable(1, vw.get()));
            vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
          }
        } finally {
          reader.close();
          writer.close();
        }
      }
    }
    return canopyConfiguration;
  }
}
