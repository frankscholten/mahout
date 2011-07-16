/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.vectorizer.seq2sparse;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.vectorizer.*;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;


public class SequenceFilesToSparseVectorsDriverTest extends MahoutTestCase {

  private static final int NUM_DOCS = 100;
  
  private Configuration conf;
  private Path inputPath;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    inputPath = getTestTempFilePath("documents/docs.file");
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, inputPath, Text.class, Text.class);

    RandomDocumentGenerator gen = new RandomDocumentGenerator();
    
    for (int i = 0; i < NUM_DOCS; i++) {
      writer.append(new Text("Document::ID::" + i), new Text(gen.getRandomDocument()));
    }
    writer.close();
  }

  @Test
  public void testRun() throws Exception {
    SequenceFilesToSparseVectors seq2Sparse = createMock(SequenceFilesToSparseVectors.class);
    Class<SequenceFilesToSparseVectorsDriver> driverClazz = SequenceFilesToSparseVectorsDriver.class;
    SequenceFilesToSparseVectorsDriver driver = createMock(driverClazz, driverClazz.getMethod("newSeqToSparse"));
    driver.setConf(new Configuration());
    SequenceFilesToSparseVectorsConfiguration seq2SparseConfig = createMock(SequenceFilesToSparseVectorsConfiguration.class);

    Path outputPath = new Path("output");
    int minSupport = 10;
    String analyzerClassName = StubAnalyzer.class.getName();
    int chunkSize = 100;
    Weight weight = new TFIDF();
    int minDf = 5;
    int maxPercent = 25;
    float minLLR = 1000;
    int numReducers = 10;
    String norm = "3";
    int maxNGramSize = 2;

    expect(driver.newSeqToSparse()).andReturn(seq2Sparse);

    String[] seq2sparseParams = new String[] {
      "-i", inputPath.toString(),
      "-o", outputPath.toString(),
      "-s", String.valueOf(minSupport),
      "-a", analyzerClassName,
      "-chunk", String.valueOf(chunkSize),
      "-wt", weight.getClass().getSimpleName(),
      "-md", String.valueOf(minDf),
      "-x", String.valueOf(maxPercent),
      "-ml", String.valueOf(minLLR),
      "-nr", String.valueOf(numReducers),
      "-n", norm,
      "-lnorm",
      "-ng", String.valueOf(maxNGramSize),
      "-seq",
      "-nv",
      "-ow"
    };

    Capture<SequenceFilesToSparseVectorsConfiguration> configCapture = new Capture<SequenceFilesToSparseVectorsConfiguration>();
    seq2Sparse.convert(capture(configCapture));
    expectLastCall();

    replay(driver, seq2Sparse, seq2SparseConfig);

    driver.run(seq2sparseParams);

    SequenceFilesToSparseVectorsConfiguration capture = configCapture.getValue();

    verify(driver, seq2Sparse, seq2SparseConfig);

    assertEquals(inputPath, capture.getInputPath());
    assertEquals(outputPath, capture.getOutputPath());
    assertEquals(minSupport, capture.getMinSupport());
    assertEquals(analyzerClassName, capture.getAnalyzerClassName());
    assertEquals(chunkSize, capture.getChunkSize());
    assertTrue(capture.isProcessIdf());
    assertEquals(minDf, capture.getMinDf());
    assertEquals(maxPercent, capture.getMaxDfPercent());
    assertEquals(minLLR, capture.getMinLLR(), EPSILON);
    assertEquals(numReducers, capture.getNumReducers());
    assertEquals(Float.parseFloat(norm), capture.getNorm(), EPSILON);
    assertTrue(capture.isLogNormalize());
    assertEquals(maxNGramSize, capture.getMaxNGramSize());
    assertTrue(capture.isOutputSequentialAccessVectors());
    assertTrue(capture.isOutputNamedVectors());
    assertTrue(capture.isOutputOverwrite());
  }

  @Test
  public void testCreateTermFrequencyVectors() throws Exception {
    runTest(false, false);
  }

  @Test
  public void testCreateTermFrequencyVectorsNam() throws Exception {
    runTest(false, true);
  }
  
  @Test
  public void testCreateTermFrequencyVectorsSeq() throws Exception {
    runTest(true, false);
  }
  
  @Test
  public void testCreateTermFrequencyVectorsSeqNam() throws Exception {
    runTest(true, true);
  }
  
  private void runTest(boolean sequential, boolean named) throws Exception {
    Path outputPath = getTestTempFilePath("output");

    
    List<String> argList = new LinkedList<String>();
    argList.add("-i");
    argList.add(inputPath.toString());
    argList.add("-o");
    argList.add(outputPath.toString());
    
    if (sequential) {
      argList.add("-seq");
    }
    
    if (named) {
      argList.add("-nv");
    }
    
    String[] args = argList.toArray(new String[argList.size()]);
    
    SequenceFilesToSparseVectorsDriver.main(args);

    Path tfVectors = new Path(outputPath, "tf-vectors");
    Path tfidfVectors = new Path(outputPath, "tfidf-vectors");
    
    DictionaryVectorizerTest.validateVectors(conf, NUM_DOCS, tfVectors, sequential, named);
    DictionaryVectorizerTest.validateVectors(conf, NUM_DOCS, tfidfVectors, sequential, named);
  }

}
