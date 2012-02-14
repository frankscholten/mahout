package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
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
import static org.junit.Assert.assertEquals;

public class LuceneSegmentRecordReaderTest {

  private LuceneSegmentRecordReader recordReader;
  private Configuration configuration;
  private String indexPath;

  @Before
  public void before() throws IOException, InterruptedException {
    indexPath = "index";

    LuceneIndexToSequenceFilesConfiguration lucene2SeqConf = new LuceneIndexToSequenceFilesConfiguration(new Configuration(), new Path(indexPath), new Path("output"), "id", "field");
    configuration = lucene2SeqConf.serializeInConfiguration();

    FSDirectory directory = FSDirectory.open(new File(indexPath));
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
    LuceneSegmentInputSplit inputSplit = new LuceneSegmentInputSplit(segmentInfo.name, segmentInfo.sizeInBytes(true));

    TaskAttemptContext context = new TaskAttemptContext(configuration, new TaskAttemptID());

    recordReader = new LuceneSegmentRecordReader();
    recordReader.initialize(inputSplit, context);
  }

  @After
  public void after() throws IOException {
    HadoopUtil.delete(configuration, new Path(indexPath));
  }

  @Test
  public void testKey() throws Exception {
    recordReader.nextKeyValue();
    assertEquals("0", recordReader.getCurrentKey().toString());
    recordReader.nextKeyValue();
    assertEquals("1", recordReader.getCurrentKey().toString());
    recordReader.nextKeyValue();
    assertEquals("2", recordReader.getCurrentKey().toString());
  }

  @Test
  public void testGetCurrentValue() throws Exception {
    assertEquals(NullWritable.get(), recordReader.getCurrentValue());
  }
}
