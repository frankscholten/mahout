package org.apache.mahout.vectorizer.seq2sparse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.junit.Before;
import org.junit.Test;

public class SequenceFilesToSparseVectorsConfigurationTest extends MahoutTestCase {

  private Path input;
  private Path output;
  private Configuration configuration;
  private SequenceFilesToSparseVectorsConfiguration seqToSparseConfiguration;

  @Before
  public void before() {
    input = new Path("input");
    output = new Path("target");
    configuration = new Configuration();

    seqToSparseConfiguration = new SequenceFilesToSparseVectorsConfiguration(configuration, input, output);
  }

  @Test
  public void testConstructor_mandatoryValues() {
    assertEquals(input, seqToSparseConfiguration.getInputPath());
    assertEquals(output, seqToSparseConfiguration.getOutputPath());
    assertEquals(configuration, seqToSparseConfiguration.getConfiguration());
  }

  @Test
  public void testConstructor_defaultValues() {
    assertTrue(seqToSparseConfiguration.getAnalyzer() instanceof DefaultAnalyzer);
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_CHUNK_SIZE, seqToSparseConfiguration.getChunkSize());
    assertFalse(seqToSparseConfiguration.isLogNormalize());
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_MAX_DF_PERCENT, seqToSparseConfiguration.getMaxDfPercent());
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_MAX_NGRAM_SIZE, seqToSparseConfiguration.getMaxNGramSize());
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_MIN_DF, seqToSparseConfiguration.getMinDf());
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_MIN_LLR, seqToSparseConfiguration.getMinLLR(), EPSILON);
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_MIN_SUPPORT, seqToSparseConfiguration.getMinSupport());
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_NORM, seqToSparseConfiguration.getNorm(), EPSILON);
    assertEquals(SequenceFilesToSparseVectorsConfiguration.DEFAULT_NUM_REDUCERS, seqToSparseConfiguration.getNumReducers());
    assertFalse(seqToSparseConfiguration.isOutputOverwrite());
    assertFalse(seqToSparseConfiguration.isOutputSequentialAccessVectors());
    assertFalse(seqToSparseConfiguration.isOutputNamedVectors());
    assertTrue(seqToSparseConfiguration.isProcessIdf());
  }

  @Test
  public void testSetters() {
    String analyzerClassName = StubAnalyzer.class.getName();
    int chunkSize = 200;
    boolean logNormalize = true;
    int maxDfPercent = 50;
    int maxNGramSize = 3;
    int minDf = 15;
    float minLLR = 5000;
    int minSupport = 5;
    float norm = 2;
    int numReducers = 10;
    boolean outputNamedVectors = true;
    boolean outputOverwrite = true;
    boolean outputSequentialAccessVectors = true;
    boolean processIdf = true;

    seqToSparseConfiguration.setAnalyzerClassName(analyzerClassName);
    seqToSparseConfiguration.setChunkSize(chunkSize);
    seqToSparseConfiguration.setLogNormalize(logNormalize);
    seqToSparseConfiguration.setMaxDfPercent(maxDfPercent);
    seqToSparseConfiguration.setMaxNGramSize(maxNGramSize);
    seqToSparseConfiguration.setMinDf(minDf);
    seqToSparseConfiguration.setMinLLR(minLLR);
    seqToSparseConfiguration.setMinSupport(minSupport);
    seqToSparseConfiguration.setNorm(norm);
    seqToSparseConfiguration.setNumReducers(numReducers);
    seqToSparseConfiguration.setOutputNamedVectors(outputNamedVectors);
    seqToSparseConfiguration.setOutputOverwrite(outputOverwrite);
    seqToSparseConfiguration.setOutputSequentialAccessVectors(outputSequentialAccessVectors);
    seqToSparseConfiguration.setProcessIdf(processIdf);

    assertEquals(analyzerClassName, seqToSparseConfiguration.getAnalyzerClassName());
    assertTrue(seqToSparseConfiguration.getAnalyzer() instanceof StubAnalyzer);
    assertEquals(chunkSize, seqToSparseConfiguration.getChunkSize());
    assertEquals(logNormalize, seqToSparseConfiguration.isLogNormalize());
    assertEquals(maxDfPercent, seqToSparseConfiguration.getMaxDfPercent());
    assertEquals(maxNGramSize, seqToSparseConfiguration.getMaxNGramSize());
    assertEquals(minDf, seqToSparseConfiguration.getMinDf());
    assertEquals(minLLR, seqToSparseConfiguration.getMinLLR(), EPSILON);
    assertEquals(minSupport, seqToSparseConfiguration.getMinSupport());
    assertEquals(norm, seqToSparseConfiguration.getNorm(), EPSILON);
    assertEquals(numReducers, seqToSparseConfiguration.getNumReducers());
    assertEquals(outputOverwrite, seqToSparseConfiguration.isOutputOverwrite());
    assertEquals(outputSequentialAccessVectors, seqToSparseConfiguration.isOutputSequentialAccessVectors());
    assertEquals(outputNamedVectors, seqToSparseConfiguration.isOutputNamedVectors());
    assertEquals(processIdf, seqToSparseConfiguration.isProcessIdf());
  }
}
