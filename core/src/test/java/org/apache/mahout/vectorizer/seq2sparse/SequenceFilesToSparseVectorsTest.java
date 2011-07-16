package org.apache.mahout.vectorizer.seq2sparse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.vectorizer.DictionaryVectorizerTest;
import org.apache.mahout.vectorizer.RandomDocumentGenerator;
import org.junit.Test;

public class SequenceFilesToSparseVectorsTest extends MahoutTestCase {

  @Test
  public void testConvert() throws Exception {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);
    Path inputPath = getTestTempFilePath("documents/docs.file");
    Path outputPath = getTestTempFilePath("output");

    SequenceFile.Writer writer = new SequenceFile.Writer(fs, configuration, inputPath, Text.class, Text.class);

    RandomDocumentGenerator gen = new RandomDocumentGenerator();
    int numDocs = 100;
    for (int i = 0; i < numDocs; i++) {
      writer.append(new Text("Document::ID::" + i), new Text(gen.getRandomDocument()));
    }
    writer.close();

    SequenceFilesToSparseVectorsConfiguration seqToSparseConfiguration = new SequenceFilesToSparseVectorsConfiguration(configuration, inputPath, outputPath);
    SequenceFilesToSparseVectors sequenceFilesToSparseVectors = new SequenceFilesToSparseVectors();
    sequenceFilesToSparseVectors.convert(seqToSparseConfiguration);

    Path tfVectors = new Path(outputPath, "tf-vectors");
    Path tfidfVectors = new Path(outputPath, "tfidf-vectors");

    DictionaryVectorizerTest.validateVectors(configuration, numDocs, tfVectors, false, false);
    DictionaryVectorizerTest.validateVectors(configuration, numDocs, tfidfVectors, false, false);
  }
}
