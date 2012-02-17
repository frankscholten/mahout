package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link InputFormat} implementation which splits a Lucene index at the segment level.
 */
public class LuceneSegmentInputFormat extends InputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(LuceneSegmentInputFormat.class);

  @Override
  public List<LuceneSegmentInputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(directory);

    List<LuceneSegmentInputSplit> inputSplits = new ArrayList<LuceneSegmentInputSplit>();
    for (SegmentInfo segmentInfo : segmentInfos.asList()) {
      LuceneSegmentInputSplit inputSplit = new LuceneSegmentInputSplit(segmentInfo.name, segmentInfo.sizeInBytes(true));
      inputSplits.add(inputSplit);
      LOG.info("Created {} byte input split for segment {}", segmentInfo.sizeInBytes(true), segmentInfo.name);
    }

    return inputSplits;
  }

  @Override
  public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    LuceneSegmentRecordReader luceneSegmentRecordReader = new LuceneSegmentRecordReader();
    luceneSegmentRecordReader.initialize(inputSplit, context);
    return luceneSegmentRecordReader;
  }
}
