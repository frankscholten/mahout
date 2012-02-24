package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    LuceneStorageConfiguration lucene2SeqConfiguration = new LuceneStorageConfiguration(configuration);

    List<LuceneSegmentInputSplit> inputSplits = new ArrayList<LuceneSegmentInputSplit>();

    List<Path> indexPaths = lucene2SeqConfiguration.getIndexPaths();
    for (Path indexPath : indexPaths) {
      FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), indexPath, false, configuration);
      SegmentInfos segmentInfos = new SegmentInfos();
      segmentInfos.read(directory);

      for (SegmentInfo segmentInfo : segmentInfos.asList()) {
        LuceneSegmentInputSplit inputSplit = new LuceneSegmentInputSplit(indexPath, segmentInfo.name, segmentInfo.sizeInBytes(true));
        inputSplits.add(inputSplit);
        LOG.info("Created {} byte input split for index '{}' segment {}", new Object[] {segmentInfo.sizeInBytes(true), indexPath.toUri(), segmentInfo.name});
      }
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
