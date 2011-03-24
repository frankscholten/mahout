package org.apache.mahout.utils.vectors.lucene;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.utils.vectors.io.JWriterVectorWriter;
import org.apache.mahout.utils.vectors.io.SequenceFileVectorWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.mahout.vectorizer.Weight;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests configuration of creating Mahout vectors from a Lucene index
 */
public class LuceneConverterConfigurationTest {

  private LuceneConverterConfiguration luceneConfig;
  private File indexDirectory;
  private Path outputVectors;
  private String field;

  @Before
  public void setup() {
    indexDirectory = new File("index");
    if (!indexDirectory.mkdir()) {
      fail("Could not create index directory");
    }
    indexDirectory.deleteOnExit();

    outputVectors = new Path("outputVectors");
    field = "field";

    luceneConfig = new LuceneConverterConfiguration(indexDirectory, outputVectors, field);
  }

  @After
  public void tearDown() {
    if (!indexDirectory.delete()) {
      fail("Could not delete index directory");
    }
  }

  @Test
  public void testConstructor_params() {
    assertEquals(indexDirectory, luceneConfig.getIndexDirectory());
    assertEquals(outputVectors, luceneConfig.getOutputVectors());
    assertEquals(field, luceneConfig.getField());
  }

  @Test
  public void testConstructor_defaults() {
    assertEquals(LuceneIterable.NO_NORMALIZING, luceneConfig.getNormPower());
    assertEquals(LuceneConverterConfiguration.DEFAULT_MIN_DF, luceneConfig.getMinDf());
    assertEquals(LuceneConverterConfiguration.DEFAULT_MAX_DF_PERCENTAGE, luceneConfig.getMaxDfPercentage());
    assertEquals(LuceneConverterConfiguration.DEFAULT_DELIMITER, luceneConfig.getDelimiter());
      assertEquals(LuceneConverterConfiguration.DEFAULT_MAX_VECTORS, luceneConfig.getMaxVectors());
      assertTrue(luceneConfig.getVectorWriter() instanceof SequenceFileVectorWriter);
      assertTrue(luceneConfig.getWeight() instanceof TFIDF);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructor_nullIndexDirectory() {
    new LuceneConverterConfiguration(null, outputVectors, field);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_indexDirectoryNotADirectory() {
    new LuceneConverterConfiguration(new File("file"), outputVectors, field);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructor_nullOutputVectors() {
    new LuceneConverterConfiguration(indexDirectory, null, field);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructor_nullField() {
    new LuceneConverterConfiguration(indexDirectory, outputVectors, null);
  }

  @Test
  public void testUseJsonWriter() {
    luceneConfig.useJsonVectorWriter();

    VectorWriter writer = luceneConfig.getVectorWriter();

    assertTrue(writer instanceof JWriterVectorWriter);
  }

  @Test
  public void testUseSeqFileWriter() {
    luceneConfig.useSeqFileWriter();

    VectorWriter writer = luceneConfig.getVectorWriter();

    assertTrue(writer instanceof SequenceFileVectorWriter);
  }

  @Test
  public void testSetters() {
    String delimiter = "\t";
    String idField = "idField";
    int maxVectors = 10;
    File outputDictionary = new File("dictionary");
    double norm = LuceneIterable.NO_NORMALIZING;
    int maxDfPercentage = 100;
    Weight weight = new TFIDF();
    int minDf = 5;

    luceneConfig.setDelimiter(delimiter);
    luceneConfig.setIdField(idField);
    luceneConfig.setMaxVectors(maxVectors);
    luceneConfig.setOutputDictionary(outputDictionary);
    luceneConfig.setNormPower(norm);
    luceneConfig.setMaxDfPercentage(maxDfPercentage);
    luceneConfig.setWeight(weight);
    luceneConfig.setMinDf(minDf);

    assertEquals(delimiter, luceneConfig.getDelimiter());
    assertEquals(idField, luceneConfig.getIdField());
    assertEquals(maxVectors, luceneConfig.getMaxVectors());
    assertEquals(outputDictionary, luceneConfig.getOutputDictionary());
    assertEquals(norm, luceneConfig.getNormPower());
    assertEquals(maxDfPercentage, luceneConfig.getMaxDfPercentage());
    assertEquals(weight, luceneConfig.getWeight());
    assertEquals(minDf, luceneConfig.getMinDf());
  }
}