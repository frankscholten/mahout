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

import org.apache.hadoop.fs.Path;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.IOException;

/**
 * Converts a given set of sequence files to sparse vectors.
 */
public class SequenceFilesToSparseVectors {

  public void convert(SequenceFilesToSparseVectorsConfiguration seqToSparseConfig) throws ClassNotFoundException, IOException, InterruptedException {
    Path tokenizedPath = new Path(seqToSparseConfig.getOutputPath(), DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
    DocumentProcessor.tokenizeDocuments(seqToSparseConfig.getInputPath(), seqToSparseConfig.getAnalyzer().getClass(), tokenizedPath, seqToSparseConfig.getConfiguration());

    if (!seqToSparseConfig.isProcessIdf()) {
      DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, seqToSparseConfig.getOutputPath(), seqToSparseConfig.getConfiguration(), seqToSparseConfig.getMinSupport(), seqToSparseConfig.getMaxNGramSize(),
          seqToSparseConfig.getMinLLR(), seqToSparseConfig.getNorm(), seqToSparseConfig.isLogNormalize(), seqToSparseConfig.getNumReducers(), seqToSparseConfig.getChunkSize(), seqToSparseConfig.isOutputSequentialAccessVectors(), seqToSparseConfig.isOutputNamedVectors());
    } else if (seqToSparseConfig.isProcessIdf()) {
      DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, seqToSparseConfig.getOutputPath(), seqToSparseConfig.getConfiguration(), seqToSparseConfig.getMinSupport(), seqToSparseConfig.getMaxNGramSize(),
        seqToSparseConfig.getMinLLR(), -1.0f, false, seqToSparseConfig.getNumReducers(), seqToSparseConfig.getChunkSize(), seqToSparseConfig.isOutputSequentialAccessVectors(), seqToSparseConfig.isOutputNamedVectors());

      TFIDFConverter.processTfIdf(
          new Path(seqToSparseConfig.getOutputPath(), DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
          seqToSparseConfig.getOutputPath(), seqToSparseConfig.getConfiguration(), seqToSparseConfig.getChunkSize(), seqToSparseConfig.getMinDf(), seqToSparseConfig.getMaxDfPercent(), seqToSparseConfig.getNorm(), seqToSparseConfig.isLogNormalize(),
          seqToSparseConfig.isOutputSequentialAccessVectors(), seqToSparseConfig.isOutputNamedVectors(), seqToSparseConfig.getNumReducers());
    }
  }
}
