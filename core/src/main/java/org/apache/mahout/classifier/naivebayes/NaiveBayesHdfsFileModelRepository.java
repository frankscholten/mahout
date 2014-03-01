package org.apache.mahout.classifier.naivebayes;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.*;import org.apache.mahout.model.ModelRepository;

import java.io.IOException;

/**
 * Repository that retrieves and stores Mahout models from a HDFS file.
 */
public class NaiveBayesHdfsFileModelRepository implements ModelRepository<NaiveBayesModel> {

  private Path modelPath;
  private Configuration configuration;

  public NaiveBayesHdfsFileModelRepository(Path modelPath, Configuration configuration) {
    this.modelPath = modelPath;
    this.configuration = configuration;
  }

  @Override
  public NaiveBayesModel readModel() {
    try {
      FileSystem fs = modelPath.getFileSystem(configuration);

      Vector weightsPerLabel = null;
      Vector perLabelThetaNormalizer = null;
      Vector weightsPerFeature = null;
      Matrix weightsPerLabelAndFeature;
      float alphaI;

      FSDataInputStream in = fs.open(new Path(modelPath, "naiveBayesModel.bin"));
      try {
        alphaI = in.readFloat();
        weightsPerFeature = VectorWritable.readVector(in);
        weightsPerLabel = new DenseVector(VectorWritable.readVector(in));
        perLabelThetaNormalizer = new DenseVector(VectorWritable.readVector(in));

        weightsPerLabelAndFeature = new SparseRowMatrix(weightsPerLabel.size(), weightsPerFeature.size());
        for (int label = 0; label < weightsPerLabelAndFeature.numRows(); label++) {
          weightsPerLabelAndFeature.assignRow(label, VectorWritable.readVector(in));
        }
      } finally {
        Closeables.close(in, true);
      }
      NaiveBayesModel model = new NaiveBayesModel(weightsPerLabelAndFeature, weightsPerFeature, weightsPerLabel,
          perLabelThetaNormalizer, alphaI);
      model.validate();
      return model;
    } catch (IOException e) {
      throw new RuntimeException("Could not read model from HDFS: ", e);
    }
  }

  @Override
  public void writeModel(NaiveBayesModel model) {
    try {
      FileSystem fs = modelPath.getFileSystem(configuration);
      FSDataOutputStream out = fs.create(new Path(modelPath, "naiveBayesModel.bin"));
      try {
        out.writeFloat(model.alphaI());
        VectorWritable.writeVector(out, model.weightsPerFeature);
        VectorWritable.writeVector(out, model.weightsPerLabel);
        VectorWritable.writeVector(out, model.perlabelThetaNormalizer);
        for (int row = 0; row < model.weightsPerLabelAndFeature.numRows(); row++) {
          VectorWritable.writeVector(out, model.weightsPerLabelAndFeature.viewRow(row));
        }
      } finally {
        Closeables.close(out, false);
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not read model from HDFS: ", e);
    }
  }
}
