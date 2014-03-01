package org.apache.mahout.classifier.naivebayes;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.training.ThetaMapper;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.model.ModelRepository;

/**
 * Reads NaiveBayesModels from a HDFS folder structure.
 */
public class NaiveBayesHdfsFolderModelRepository implements ModelRepository<NaiveBayesModel> {

  private final Path basePath;
  private final Configuration configuration;

  public NaiveBayesHdfsFolderModelRepository(Path basePath, Configuration configuration) {
    this.basePath = basePath;
    this.configuration = configuration;
  }

  public NaiveBayesModel readModel() {
    float alphaI = configuration.getFloat(ThetaMapper.ALPHA_I, 1.0f);

    // read feature sums and label sums
    Vector scoresPerLabel = null;
    Vector scoresPerFeature = null;
    for (Pair<Text,VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
        new Path(basePath, TrainNaiveBayesJob.WEIGHTS), PathType.LIST, PathFilters.partFilter(), configuration)) {
      String key = record.getFirst().toString();
      VectorWritable value = record.getSecond();
      if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_FEATURE)) {
        scoresPerFeature = value.get();
      } else if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_LABEL)) {
        scoresPerLabel = value.get();
      }
    }

    Preconditions.checkNotNull(scoresPerFeature);
    Preconditions.checkNotNull(scoresPerLabel);

    Matrix scoresPerLabelAndFeature = new SparseMatrix(scoresPerLabel.size(), scoresPerFeature.size());
    for (Pair<IntWritable,VectorWritable> entry : new SequenceFileDirIterable<IntWritable,VectorWritable>(
        new Path(basePath, TrainNaiveBayesJob.SUMMED_OBSERVATIONS), PathType.LIST, PathFilters.partFilter(), configuration)) {
      scoresPerLabelAndFeature.assignRow(entry.getFirst().get(), entry.getSecond().get());
    }

    Vector perlabelThetaNormalizer = scoresPerLabel.like();
    /* for (Pair<Text,VectorWritable> entry : new SequenceFileDirIterable<Text,VectorWritable>(
        new Path(base, TrainNaiveBayesJob.THETAS), PathType.LIST, PathFilters.partFilter(), conf)) {
      if (entry.getFirst().toString().equals(TrainNaiveBayesJob.LABEL_THETA_NORMALIZER)) {
        perlabelThetaNormalizer = entry.getSecond().get();
      }
    }

    Preconditions.checkNotNull(perlabelThetaNormalizer);
    */
    return new NaiveBayesModel(scoresPerLabelAndFeature, scoresPerFeature, scoresPerLabel, perlabelThetaNormalizer,
        alphaI);
  }

  @Override
  public void writeModel(NaiveBayesModel model) {
    throw new UnsupportedOperationException("Writing to a HDFS folder structure is not implemented");
  }

}
