package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link InputFormat} implementation which splits lucene query result into logical splits.
 */
public class LuceneIndexInputFormat extends InputFormat<NullWritable, NullWritable> {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration configuration = jobContext.getConfiguration();

    LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);
    IndexReader indexReader = IndexReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    TopDocs topDocs = indexSearcher.search(lucene2SeqConfiguration.getQuery(), lucene2SeqConfiguration.getMaxHits());

    List<InputSplit> inputSplits = new ArrayList<InputSplit>();

    int hits = topDocs.scoreDocs.length;

    if (hits == 0) {
      return Collections.EMPTY_LIST;
    }
    
    int numMappers = Integer.valueOf(configuration.get("mapred.map.tasks"));
    int length = hits / numMappers;
    for (int i = 0; i < numMappers; i++) {
      inputSplits.add(new LuceneInputSplit(i * length, length));
    }

    return inputSplits;
  }

  @Override
  public LuceneIndexRecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    LuceneIndexRecordReader recordReader = new LuceneIndexRecordReader();
    recordReader.initialize(inputSplit, taskAttemptContext);
    return recordReader;
  }
}
