package org.apache.mahout.utils.vectors.lucene;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.TermInfo;
import org.apache.mahout.utils.vectors.io.JWriterVectorWriter;
import org.apache.mahout.utils.vectors.io.SequenceFileVectorWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;
import org.apache.mahout.vectorizer.Weight;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Configuration for {@link LuceneVectorConverter}
 */
public class LuceneVectorConverterConfiguration {

  private File indexDirectory;
  private Path outputVectors;
  private String idField;
  private String field;
  private String delimiter;
  private long maxVectors;
  private File outputDictionary;
  private double normPower;
  private int maxDfPercentage;
  private Weight weight;
  private int minDf;
  private VectorWriter vectorWriter;

  public LuceneVectorConverterConfiguration(File indexDirectory, Path outputVectors, String field) {
    Preconditions.checkNotNull(indexDirectory, "IndexDirectory cannot be null");
    Preconditions.checkNotNull(outputVectors, "Outputvectors cannot be null");
    Preconditions.checkNotNull(field, "Field cannot be null");

    if (!indexDirectory.isDirectory()) {
      throw new IllegalArgumentException("Lucene directory: " + indexDirectory.getAbsolutePath() + " does not exist or is not a directory");
    }

    this.indexDirectory = indexDirectory;
    this.outputVectors = outputVectors;
    this.field = field;
    this.normPower = LuceneIterable.NO_NORMALIZING;

    useSeqFileWriter();
  }

  public Path getOutputVectors() {
    return outputVectors;
  }

  public String getField() {
    return field;
  }

  public File getIndexDirectory() {
    return indexDirectory;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  public void setMaxVectors(long maxVectors) {
    this.maxVectors = maxVectors;
  }

  public void setOutputDictionary(File outputDictionary) {
    this.outputDictionary = outputDictionary;
  }

  public void setNormPower(double normPower) {
    this.normPower = normPower;
  }

  public void setMaxDfPercentage(int maxDfPercentage) {
    this.maxDfPercentage = maxDfPercentage;
  }

  public void setWeight(Weight weight) {
    this.weight = weight;
  }

  public void setMinDf(int minDf) {
    this.minDf = minDf;
  }

  public String getIdField() {
    return idField;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public long getMaxVectors() {
    return maxVectors;
  }

  public File getOutputDictionary() {
    return outputDictionary;
  }

  public double getNormPower() {
    return normPower;
  }

  public int getMaxDfPercentage() {
    return maxDfPercentage;
  }

  public Weight getWeight() {
    return weight;
  }

  public int getMinDf() {
    return minDf;
  }

  public void useJsonVectorWriter() {
    try {
      Writer writer = new OutputStreamWriter(new FileOutputStream(new File(outputVectors.toString())), Charset.forName("UTF8"));
      vectorWriter = new JWriterVectorWriter(writer);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Could not access output vector path", e);
    }
  }

  public void useSeqFileWriter() {
    // TODO: Make this parameter driven

    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs, conf, outputVectors, LongWritable.class, VectorWritable.class);
      vectorWriter = new SequenceFileVectorWriter(seqWriter);
    } catch (IOException e) {
      throw new RuntimeException("Could not access output vector path", e);
    }
  }

  public VectorWriter getVectorWriter() {
    return vectorWriter;
  }

  public LuceneIterable createLuceneIterable() {
    try {
      Directory dir = FSDirectory.open(indexDirectory);
      IndexReader reader = IndexReader.open(dir, true);
      TermInfo termInfo = new CachedTermInfo(reader, field, minDf, maxDfPercentage);
      VectorMapper mapper = new TFDFMapper(reader, weight, termInfo);
      return new LuceneIterable(reader, idField, field, mapper, LuceneIterable.NO_NORMALIZING);
    } catch (IOException e) {
      throw new RuntimeException("Could not create " + LuceneIterable.class.getSimpleName(), e);
    }
  }
}
