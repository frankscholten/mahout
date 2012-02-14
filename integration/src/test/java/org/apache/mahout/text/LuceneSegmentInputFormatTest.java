package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.text.doc.SimpleDocument;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;

public class LuceneSegmentInputFormatTest {

  private LuceneSegmentInputFormat inputFormat;
  private JobContext jobContext;
  private Path indexPath;
  private Configuration conf;

  @Before
  public void before() throws IOException {
    inputFormat = new LuceneSegmentInputFormat();
    indexPath = new Path("index");

    LuceneIndexToSequenceFilesConfiguration lucene2SeqConf = new LuceneIndexToSequenceFilesConfiguration(new Configuration(), indexPath, new Path("output"), "id", "field");
    conf = lucene2SeqConf.serializeInConfiguration();

    jobContext = new JobContext(conf, new JobID());
  }
  
  @After
  public void after() throws IOException {
    HadoopUtil.delete(conf, indexPath);
  }

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    FSDirectory directory = FSDirectory.open(new File(indexPath.toString()));
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new DefaultAnalyzer()));

    SimpleDocument doc1 = new SimpleDocument("1", "This is simple document 1");
    SimpleDocument doc2 = new SimpleDocument("2", "This is simple document 2");
    SimpleDocument doc3 = new SimpleDocument("3", "This is simple document 3");
    List<SimpleDocument> documents = asList(doc1, doc2, doc3);

    for (SimpleDocument simpleDocument : documents) {
      indexWriter.addDocument(simpleDocument.asLuceneDocument());
    }

    indexWriter.commit();
    indexWriter.close();

    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(directory);

    SegmentInfo segmentInfo = segmentInfos.asList().get(0);
    long segmentSize = segmentInfo.sizeInBytes(true);

    List<LuceneSegmentInputSplit> splits = inputFormat.getSplits(jobContext);
    
    assertEquals(1, splits.size());
    assertEquals(segmentSize, splits.get(0).getLength());
  }
}
