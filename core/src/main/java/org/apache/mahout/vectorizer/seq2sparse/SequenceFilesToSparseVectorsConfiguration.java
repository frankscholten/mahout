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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;

/**
 * Holds all mandatory, default and optional configuration for {@link SequenceFilesToSparseVectors}
 */
public class SequenceFilesToSparseVectorsConfiguration {

  public static final int DEFAULT_CHUNK_SIZE = 100;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_DF_PERCENT = 99;
  public static final int DEFAULT_MIN_DF = 1;
  public static final float DEFAULT_MIN_LLR = LLRReducer.DEFAULT_MIN_LLR;
  public static final int DEFAULT_MIN_SUPPORT = 2;
  public static final float DEFAULT_NORM = PartialVectorMerger.NO_NORMALIZING;
  public static final int DEFAULT_NUM_REDUCERS = 1;

  private Configuration configuration;
  private Path inputPath;
  private Path outputPath;

  private Analyzer analyzer;
  private String analyzerClassName;
  private int chunkSize;
  private boolean logNormalize;
  private int minSupport;
  private int minDf;
  private float minLLR;
  private int maxDfPercent;
  private int maxNGramSize;
  private float norm;
  private int numReducers;
  private boolean outputSequentialAccessVectors;
  private boolean outputNamedVectors;
  private boolean outputOverwrite;
  private boolean processIdf;

  public SequenceFilesToSparseVectorsConfiguration(Configuration configuration, Path inputPath, Path outputPath) {
    this.configuration = configuration;
    this.inputPath = inputPath;
    this.outputPath = outputPath;

    setAnalyzerClassName(DefaultAnalyzer.class.getName());
    setChunkSize(DEFAULT_CHUNK_SIZE);
    setMinSupport(DEFAULT_MIN_SUPPORT);
    setMaxNGramSize(DEFAULT_MAX_NGRAM_SIZE);
    setMinLLR(DEFAULT_MIN_LLR);
    setProcessIdf(true);
    setMinDf(DEFAULT_MIN_DF);
    setMaxDfPercent(DEFAULT_MAX_DF_PERCENT);
    setNorm(DEFAULT_NORM);
    setNumReducers(DEFAULT_NUM_REDUCERS);
  }

  public Path getInputPath() {
    return inputPath;
  }

  public Path getOutputPath() {
    return outputPath;
  }

  public void setMinSupport(int minSupport) {
    this.minSupport = minSupport;
  }

  public int getMinSupport() {
    return minSupport;
  }

  public String getAnalyzerClassName() {
    return analyzerClassName;
  }

  public void setAnalyzerClassName(String analyzerClassName) {
    this.analyzerClassName = analyzerClassName;
    try {
      Class analyzerClass = Class.forName(analyzerClassName).asSubclass(Analyzer.class);
      this.analyzer = (Analyzer) analyzerClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Analyzer class " + analyzerClassName + " not found");
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not instantiate analyzer of class " + analyzerClassName);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public void setProcessIdf(boolean processIdf) {
    this.processIdf = processIdf;
  }

  public boolean isProcessIdf() {
    return this.processIdf;
  }

  public int getMinDf() {
    return minDf;
  }

  public void setMinDf(int minDf) {
    this.minDf = minDf;
  }

  public int getMaxDfPercent() {
    return maxDfPercent;
  }

  public void setMaxDfPercent(int maxDfPercent) {
    this.maxDfPercent = maxDfPercent;
  }

  public float getMinLLR() {
    return minLLR;
  }

  public void setMinLLR(float minLLR) {
    this.minLLR = minLLR;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  public float getNorm() {
    return norm;
  }

  public void setNorm(float norm) {
    this.norm = norm;
  }

  public boolean isLogNormalize() {
    return logNormalize;
  }

  public void setLogNormalize(boolean logNormalize) {
    this.logNormalize = logNormalize;
  }

  public int getMaxNGramSize() {
    return maxNGramSize;
  }

  public void setMaxNGramSize(int maxNGramSize) {
    this.maxNGramSize = maxNGramSize;
  }

  public boolean isOutputSequentialAccessVectors() {
    return outputSequentialAccessVectors;
  }

  public void setOutputSequentialAccessVectors(boolean outputSequentialAccessVectors) {
    this.outputSequentialAccessVectors = outputSequentialAccessVectors;
  }

  public boolean isOutputNamedVectors() {
    return outputNamedVectors;
  }

  public void setOutputNamedVectors(boolean outputNamedVectors) {
    this.outputNamedVectors = outputNamedVectors;
  }

  public boolean isOutputOverwrite() {
    return outputOverwrite;
  }

  public void setOutputOverwrite(boolean outputOverwrite) {
    this.outputOverwrite = outputOverwrite;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public Configuration getConfiguration() {
    return configuration;
  }
}
