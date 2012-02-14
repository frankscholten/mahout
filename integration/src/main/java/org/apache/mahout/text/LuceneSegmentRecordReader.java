package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;

/**
 * {@link RecordReader} implementation for Lucene segments. Each {@link InputSplit} contains a separate Lucene segment.
 * Emits records consisting of a {@link Text} document ID and a null key.
 */
public class LuceneSegmentRecordReader extends RecordReader<Text, NullWritable> {

  public static final boolean READ_ONLY = true;

  private IndexReader reader;
  private IndexSearcher searcher;
  private Scorer scorer;

  private int nextDocId;
  private Text key = new Text();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    LuceneSegmentInputSplit inputSplit = (LuceneSegmentInputSplit) split;
    
    Configuration configuration = context.getConfiguration();
    LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(directory);

    SegmentInfo segment = getSegment(segmentInfos, inputSplit);
    Directory segmentDirectory = SegmentReader.get(READ_ONLY, segment, 1).directory();

    reader = IndexReader.open(segmentDirectory, true);
    searcher = new IndexSearcher(reader);
    Weight weight = lucene2SeqConfiguration.getQuery().createWeight(searcher);
    scorer = weight.scorer(reader, true, false);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    nextDocId = scorer.nextDoc();

    return nextDocId != Scorer.NO_MORE_DOCS;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    key.set(String.valueOf(nextDocId));
    return key;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0; // TODO: Implement with Collector
  }

  @Override
  public void close() throws IOException {
    reader.close();
    searcher.close();
  }

  private SegmentInfo getSegment(SegmentInfos segmentInfos, LuceneSegmentInputSplit inputSplit) {
    List<SegmentInfo> segmentInfoList = segmentInfos.asList();

    for (SegmentInfo segmentInfo : segmentInfoList) {
      if (segmentInfo.name.equals(inputSplit.getSegmentInfoName())) {
        return segmentInfo;
      }
    }
    return segmentInfoList.get(0);
  }
}
