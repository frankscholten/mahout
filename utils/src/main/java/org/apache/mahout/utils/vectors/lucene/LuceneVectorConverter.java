package org.apache.mahout.utils.vectors.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.mahout.utils.vectors.TermInfo;
import org.apache.mahout.utils.vectors.io.JWriterTermInfoWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Reads Lucene vectors and outputs Mahout sparse vectors based on the {@link LuceneVectorConverterConfiguration}
 */
public class LuceneVectorConverter {

  private static final Logger log = LoggerFactory.getLogger(LuceneVectorConverter.class);

  /**
   * Converts vectors of Lucene index to Mahout vectors via the given configuration.
   *
   * @param configuration configuration of the lucene index and output of mahout vector files
   */
  public void convertLuceneVectors(LuceneVectorConverterConfiguration configuration) {
    try {
      writeVectors(configuration);
      writeDictionaryFile(configuration);
    } catch (IOException e) {
      throw new RuntimeException("Could not convert lucene vectors", e);
    }
  }

  private void writeVectors(LuceneVectorConverterConfiguration configuration) throws IOException {
    VectorWriter vectorWriter = configuration.getVectorWriter();
    LuceneIterable luceneIterable = configuration.createLuceneIterable();

    long numDocs = vectorWriter.write(luceneIterable, configuration.getMaxVectors());
    vectorWriter.close();

    log.info("Wrote: {} vectors", numDocs);
  }

  private void writeDictionaryFile(LuceneVectorConverterConfiguration luceneVectorConverterConfiguration) throws IOException {
    Directory dir = FSDirectory.open(luceneVectorConverterConfiguration.getIndexDirectory());
    IndexReader reader = IndexReader.open(dir, true);
    TermInfo termInfo = new CachedTermInfo(reader, luceneVectorConverterConfiguration.getField(), luceneVectorConverterConfiguration.getMinDf(), luceneVectorConverterConfiguration.getMaxDfPercentage());

    Writer writer = new OutputStreamWriter(new FileOutputStream(luceneVectorConverterConfiguration.getOutputDictionary()), Charset.forName("UTF8"));
    JWriterTermInfoWriter tiWriter = new JWriterTermInfoWriter(writer, luceneVectorConverterConfiguration.getDelimiter(), luceneVectorConverterConfiguration.getField());
    tiWriter.write(termInfo);
    tiWriter.close();

    writer.close();

    log.info("Dictionary Output file: {}", luceneVectorConverterConfiguration.getOutputDictionary());    
  }
}
