package org.apache.mahout.clustering.kmeans;

/**
 * Runs K-Means clustering.
 */
public interface KMeansAlgorithm {

  /**
   * Runs the K-Means clustering algorithm with the given {@link KMeansConfiguration}
   *
   * @param kMeansConfiguration configuration for tuning the K-Means algorithm
   * @return the original configuration
   * @throws Exception when an exception occured during the execution of the algorithm
   */
  public KMeansConfiguration run(KMeansConfiguration kMeansConfiguration) throws Exception;

}
