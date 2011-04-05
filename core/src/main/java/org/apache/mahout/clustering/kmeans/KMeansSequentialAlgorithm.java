package org.apache.mahout.clustering.kmeans;

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
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Sequential implementation of the K-Means algorithm for creating clusters and points.
 */
public class KMeansSequentialAlgorithm implements KMeansAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(KMeansSequentialAlgorithm.class);

  public KMeansConfiguration run(KMeansConfiguration kMeansConfiguration) throws IOException, IllegalAccessException, InstantiationException {
    KMeansClusterer clusterer = new KMeansClusterer(kMeansConfiguration.getDistanceMeasure());
    Collection<Cluster> clusters = new ArrayList<Cluster>();

    KMeansUtil.configureWithClusterInfo(kMeansConfiguration.getInputClusters(), clusters);
    if (clusters.isEmpty()) {
      throw new IllegalStateException("Clusters is empty!");
    }

    boolean converged = false;
    int iteration = 1;

    Path outputBaseDir = kMeansConfiguration.getOutputclusters();

    while (!converged && iteration <= kMeansConfiguration.getMaxIterations()) {
      log.info("K-Means Iteration {}", iteration);

      FileSystem fs = FileSystem.get(kMeansConfiguration.getInputVectors().toUri(), kMeansConfiguration.getConfiguration());

      FileStatus[] statuses = fs.listStatus(kMeansConfiguration.getInputVectors(), PathFilters.logsCRCFilter());

      for (FileStatus status : statuses) {
        addPointToNearestCluster(kMeansConfiguration, clusterer, clusters, status);
      }
      converged = clusterer.testConvergence(clusters, kMeansConfiguration.getConvergenceDelta());

      Path clustersOut = new Path(outputBaseDir, AbstractCluster.CLUSTERS_DIR + iteration);

      kMeansConfiguration.setOutputClusters(clustersOut);

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
      kMeansConfiguration.setInputClusters(clustersOut);
      iteration++;
    }

    if (kMeansConfiguration.runsClustering()) {
      log.info("Clustering data");

      if (log.isInfoEnabled()) {
        log.info("Running Clustering");
        log.info("Input: {} Clusters In: {} Out: {} Distance: {}", new Object[]{kMeansConfiguration.getInputVectors(), kMeansConfiguration.getInputClusters(), kMeansConfiguration.getOutputPoints(), kMeansConfiguration.getDistanceMeasure()});
        log.info("convergence: {} Input Vectors: {}", kMeansConfiguration.getConvergenceDelta(), VectorWritable.class.getName());
      }

      FileSystem fs = FileSystem.get(kMeansConfiguration.getInputVectors().toUri(), kMeansConfiguration.getConfiguration());
      FileStatus[] parts = fs.listStatus(kMeansConfiguration.getInputVectors(), PathFilters.logsCRCFilter());

      int part = 0;
      for (FileStatus status : parts) {
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,
            kMeansConfiguration.getConfiguration(),
            new Path(kMeansConfiguration.getOutputPoints(), "part-m-" + part),
            IntWritable.class,
            WeightedVectorWritable.class);

        try {
          SequenceFileIterable<Writable, VectorWritable> iterable = new SequenceFileIterable<Writable, VectorWritable>(status.getPath(), kMeansConfiguration.getConfiguration());
          for (Pair<Writable, VectorWritable> pair : iterable){
            clusterer.emitPointToNearestCluster(pair.getSecond().get(), clusters, writer);
          }
        } finally {
          writer.close();
        }
      }
    }
    return kMeansConfiguration;
  }

  private void addPointToNearestCluster(KMeansConfiguration kMeansConfiguration, KMeansClusterer clusterer, Collection<Cluster> clusters, FileStatus status) throws IOException, InstantiationException, IllegalAccessException {
    SequenceFileIterable<Writable, VectorWritable> iterable = new SequenceFileIterable<Writable, VectorWritable>(status.getPath(), kMeansConfiguration.getConfiguration());

    for (Pair<Writable, VectorWritable> pair : iterable) {
      clusterer.addPointToNearestCluster(pair.getSecond().get(), clusters);
    }
  }
}
