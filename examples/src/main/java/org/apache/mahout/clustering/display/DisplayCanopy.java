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

package org.apache.mahout.clustering.display;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyConfiguration;
import org.apache.mahout.clustering.canopy.CanopySequentialAlgorithm;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.DenseVector;

class DisplayCanopy extends DisplayClustering {

  DisplayCanopy() {
    initialize();
    this.setTitle("Canopy Clusters (>" + (int) (significance * 100) + "% of population)");
  }

  @Override
  public void paint(Graphics g) {
    plotSampleData((Graphics2D) g);
    plotClusters((Graphics2D) g);
  }

  protected static void plotClusters(Graphics2D g2) {
    int cx = CLUSTERS.size() - 1;
    for (List<Cluster> clusters : CLUSTERS) {
      for (Cluster cluster : clusters) {
        if (isSignificant(cluster)) {
          g2.setStroke(new BasicStroke(1));
          g2.setColor(Color.BLUE);
          double[] t1 = {T1, T1};
          plotEllipse(g2, cluster.getCenter(), new DenseVector(t1));
          double[] t2 = {T2, T2};
          plotEllipse(g2, cluster.getCenter(), new DenseVector(t2));
          g2.setColor(COLORS[Math.min(DisplayClustering.COLORS.length - 1, cx)]);
          g2.setStroke(new BasicStroke(cx == 0 ? 3 : 1));
          plotEllipse(g2, cluster.getCenter(), cluster.getRadius().times(3));
        }
      }
      cx--;
    }
  }

  public static void main(String[] args) throws Exception {
    Path samples = new Path("samples");
    Path output = new Path("output");
    HadoopUtil.overwriteOutput(samples);
    HadoopUtil.overwriteOutput(output);
    RandomUtils.useTestSeed();
    generateSamples();
    writeSampleData(samples);

    CanopyConfiguration canopyConfiguration = new CanopyConfiguration(new Configuration(), samples, output);
    canopyConfiguration.setT1(T1);
    canopyConfiguration.setT2(T1);
    canopyConfiguration.setDistanceMeasure(new ManhattanDistanceMeasure());

    new CanopySequentialAlgorithm().run(canopyConfiguration);

    loadClusters(output);

    new DisplayCanopy();
  }

}
