package org.apache.mahout.text;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * {@link RecordReader} implementation which computes the size of the split but emits NullWritables since the actual
 * document is only
 */
public class LuceneIndexRecordReader extends RecordReader<NullWritable, NullWritable> {

  private LuceneInputSplit luceneInputSplit;
  
  int processedDocs = 0;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    luceneInputSplit = (LuceneInputSplit) inputSplit;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return processedDocs < luceneInputSplit.getLength();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    NullWritable nullWritable = NullWritable.get();

    processedDocs++;

    return nullWritable;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (processedDocs - luceneInputSplit.getPos()) / luceneInputSplit.getLength();
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }
}
