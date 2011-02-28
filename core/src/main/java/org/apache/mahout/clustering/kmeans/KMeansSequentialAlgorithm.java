package org.apache.mahout.clustering.kmeans;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Sequential implementation of the K-Means algorithm for creating clusters and points.
 */
public class KMeansSequentialAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(KMeansSequentialAlgorithm.class);

  public KMeansConfiguration run(KMeansConfiguration kMeansConfiguration) throws IOException, IllegalAccessException, InstantiationException {
    KMeansClusterer clusterer = new KMeansClusterer(kMeansConfiguration.getDistanceMeasure());
    Collection<Cluster> clusters = new ArrayList<Cluster>();

    KMeansUtil.configureWithClusterInfo(kMeansConfiguration.getClusterPath(), clusters);
    if (clusters.isEmpty()) {
      throw new IllegalStateException("Clusters is empty!");
    }

    boolean converged = false;
    int iteration = 1;

    Path outputBaseDir = kMeansConfiguration.getOutput();

    while (!converged && iteration <= kMeansConfiguration.getMaxIterations()) {
      log.info("K-Means Iteration {}", iteration);

      FileSystem fs = FileSystem.get(kMeansConfiguration.getInput().toUri(), kMeansConfiguration.getConfiguration());
      FileStatus[] status = fs.listStatus(kMeansConfiguration.getInput(), new OutputLogFilter());
      for (FileStatus s : status) {
        addPointToNearestCluster(kMeansConfiguration, clusterer, clusters, fs, s);
      }
      converged = clusterer.testConvergence(clusters, kMeansConfiguration.getConvergenceDelta());

      Path clustersOut = new Path(outputBaseDir, AbstractCluster.CLUSTERS_DIR + iteration);

      kMeansConfiguration.setOutput(clustersOut);

      SequenceFile.Writer writer = new SequenceFile.Writer(fs,
          kMeansConfiguration.getConfiguration(),
          new Path(clustersOut, "part-r-00000"),
          Text.class,
          Cluster.class);
      try {
        for (Cluster cluster : clusters) {
          log.debug("Writing Cluster:{} center:{} numPoints:{} radius:{} to: {}", new Object[]{cluster.getId(),
              AbstractCluster.formatVector(cluster.getCenter(), null), cluster.getNumPoints(),
              AbstractCluster.formatVector(cluster.getRadius(), null), clustersOut.getName()});
          writer.append(new Text(cluster.getIdentifier()), cluster);
        }
      } finally {
        writer.close();
      }
      kMeansConfiguration.setClusterPath(clustersOut);
      iteration++;
    }

    if (kMeansConfiguration.runsClustering()) {
      log.info("Clustering data");

      if (log.isInfoEnabled()) {
        log.info("Running Clustering");
        log.info("Input: {} Clusters In: {} Out: {} Distance: {}", new Object[]{kMeansConfiguration.getInput(), kMeansConfiguration.getClusterPath(), kMeansConfiguration.getPointsOutput(), kMeansConfiguration.getDistanceMeasure()});
        log.info("convergence: {} Input Vectors: {}", kMeansConfiguration.getConvergenceDelta(), VectorWritable.class.getName());
      }

      FileSystem fs = FileSystem.get(kMeansConfiguration.getInput().toUri(), kMeansConfiguration.getConfiguration());
      FileStatus[] statuses = fs.listStatus(kMeansConfiguration.getInput(), new OutputLogFilter());
      int part = 0;
      for (FileStatus status : statuses) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), kMeansConfiguration.getConfiguration());
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,
            kMeansConfiguration.getConfiguration(),
            new Path(kMeansConfiguration.getPointsOutput(), "part-m-" + part),
            IntWritable.class,
            WeightedVectorWritable.class);
        try {
          Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
          VectorWritable vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
          while (reader.next(key, vw)) {
            clusterer.emitPointToNearestCluster(vw.get(), clusters, writer);
            vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
          }
        } finally {
          reader.close();
          writer.close();
        }
      }
    }
    return kMeansConfiguration;
  }

  private void addPointToNearestCluster(KMeansConfiguration kMeansConfiguration, KMeansClusterer clusterer, Collection<Cluster> clusters, FileSystem fs, FileStatus s) throws IOException, InstantiationException, IllegalAccessException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, s.getPath(), kMeansConfiguration.getConfiguration());
    try {
      Writable key = reader.getKeyClass().asSubclass(Writable.class).newInstance();
      VectorWritable vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
      while (reader.next(key, vw)) {
        clusterer.addPointToNearestCluster(vw.get(), clusters);
        vw = reader.getValueClass().asSubclass(VectorWritable.class).newInstance();
      }
    } finally {
      reader.close();
    }
  }
}
