package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LuceneSegmentInputFormat extends InputFormat {

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
    }

    return inputSplits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    LuceneSegmentRecordReader luceneSegmentRecordReader = new LuceneSegmentRecordReader();
    luceneSegmentRecordReader.initialize(inputSplit, context);
    return luceneSegmentRecordReader;
  }
}
