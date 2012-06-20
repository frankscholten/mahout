package org.apache.mahout.clustering.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.knn.WeightedVector;
import org.apache.mahout.knn.means.StreamingKmeans;
import org.apache.mahout.knn.search.Searcher;
import org.apache.mahout.math.Arrays;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.MatrixUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.PathIterator;
import java.util.List;

/**
 *
 */
public class StreamingKMeansDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(StreamingKMeansDriver.class);

  public static void main(String[] args) throws Exception {
    log.info(StreamingKMeansDriver.class.getName() + " called with params " + Arrays.toString(args));

    ToolRunner.run(new Configuration(), new StreamingKMeansDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.distanceMeasureOption().create());
    addOption(DefaultOptionCreator
        .clustersInOption()
        .withDescription(
            "The input centroids, as Vectors.  Must be a SequenceFile of Writable, Cluster/Canopy.  "
                + "If k is also specified, then a random set of vectors will be selected"
                + " and written out to this path first").create());
    addOption(DefaultOptionCreator
        .numClustersOption()
        .withDescription(
            "The k in k-Means.  If specified, then a random selection of k Vectors will be chosen"
                + " as the Centroid and written to the clusters input path.").create());
    addOption(DefaultOptionCreator.convergenceOption().create());
    addOption(DefaultOptionCreator.overwriteOption().create());
    addOption(DefaultOptionCreator.clusteringOption().create());
    addOption(DefaultOptionCreator.methodOption().create());
    addOption(DefaultOptionCreator.outlierThresholdOption().create());

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

    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), output);
    }
    DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);

    if (hasOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION)) {
      clusters = RandomSeedGenerator.buildRandom(getConf(), input, clusters,
          Integer.parseInt(getOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION)), measure);
    }

    int numClusters = Integer.parseInt(getOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION));

    if (getConf() == null) {
      setConf(new Configuration());
    }

    Iterable<MatrixSlice> matrix = MatrixUtils.read(getConf(), new Path(input, "part-r-00000"));

    StreamingKmeans streamingKMeans = new StreamingKmeans();
    Searcher searcher = streamingKMeans.cluster(measure, matrix, numClusters);

    Iterable<MatrixSlice> vectors = MatrixUtils.read(getConf(), input);

    SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(getConf()), getConf(), clusters, IntWritable.class, WeightedVectorWritable.class);

    int numberOfCentroids = 1;
    IntWritable clusterId = new IntWritable();
    int i = 0;
    for (MatrixSlice vector : vectors) {
      List<WeightedVector> centroids = searcher.search(vector.vector(), numberOfCentroids);
      WeightedVector centroid = centroids.get(0);

      clusterId.set(i);
      WeightedVectorWritable weightedVectorWritable = new WeightedVectorWritable(centroid.getWeight(), centroid.getVector());
      writer.append(centroid, weightedVectorWritable);

      i++;
    }

    writer.close();

    return 0;
  }
}
