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
 * Reads Lucene vectors and outputs Mahout sparse vectors based on the {@link LuceneConverterConfiguration}
 */
public class LuceneConverter {

  private static final Logger log = LoggerFactory.getLogger(LuceneConverter.class);

  /**
   * Converts vectors of Lucene index to Mahout vectors via the given configuration.
   *
   * @param configuration configuration of the lucene index and output of mahout vector files
   */
  public void convertLuceneVectors(LuceneConverterConfiguration configuration) {
    try {
      writeVectors(configuration);
      writeDictionaryFile(configuration);
    } catch (IOException e) {
      throw new RuntimeException("Could not convert lucene vectors", e);
    }
  }

  private void writeVectors(LuceneConverterConfiguration configuration) throws IOException {
    VectorWriter vectorWriter = configuration.getVectorWriter();
    LuceneIterable luceneIterable = configuration.createLuceneIterable();

    long numDocs = vectorWriter.write(luceneIterable, configuration.getMaxVectors());
    vectorWriter.close();

    log.info("Wrote: {} vectors", numDocs);
  }

  private void writeDictionaryFile(LuceneConverterConfiguration luceneConverterConfiguration) throws IOException {
    Directory dir = FSDirectory.open(luceneConverterConfiguration.getIndexDirectory());
    IndexReader reader = IndexReader.open(dir, true);
    TermInfo termInfo = new CachedTermInfo(reader, luceneConverterConfiguration.getField(), luceneConverterConfiguration.getMinDf(), luceneConverterConfiguration.getMaxDfPercentage());

    Writer writer = new OutputStreamWriter(new FileOutputStream(luceneConverterConfiguration.getOutputDictionary()), Charset.forName("UTF8"));
    JWriterTermInfoWriter tiWriter = new JWriterTermInfoWriter(writer, luceneConverterConfiguration.getDelimiter(), luceneConverterConfiguration.getField());
    tiWriter.write(termInfo);
    tiWriter.close();

    writer.close();

    log.info("Dictionary Output file: {}", luceneConverterConfiguration.getOutputDictionary());
  }
}
