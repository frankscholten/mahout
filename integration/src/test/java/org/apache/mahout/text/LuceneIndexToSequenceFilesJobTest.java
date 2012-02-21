package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.text.doc.SingleFieldDocument;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LuceneIndexToSequenceFilesJobTest {

  private LuceneIndexToSequenceFilesJob lucene2seq;
  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConf;
  private SingleFieldDocument document1;
  private SingleFieldDocument document2;
  private SingleFieldDocument document3;
  private SingleFieldDocument document4;
  private Path index;

  @Before
  public void before() {
    lucene2seq = new LuceneIndexToSequenceFilesJob();

    Configuration configuration = new Configuration();
    index = new Path("index");
    Path seqOutputPath = new Path("seqOutputPath");

    lucene2SeqConf = new LuceneIndexToSequenceFilesConfiguration(configuration, asList(index), seqOutputPath, SingleFieldDocument.ID_FIELD, asList(SingleFieldDocument.FIELD));

    document1 = new SingleFieldDocument("1", "This is test document 1");
    document2 = new SingleFieldDocument("2", "This is test document 2");
    document3 = new SingleFieldDocument("3", "This is test document 3");
    document4 = new SingleFieldDocument("4", "This is test document 4");
  }

  @After
  public void after() throws IOException {
    HadoopUtil.delete(lucene2SeqConf.getConfiguration(), lucene2SeqConf.getSequenceFilesOutputPath());
    HadoopUtil.delete(lucene2SeqConf.getConfiguration(), lucene2SeqConf.getIndexPaths());
  }

  @Test
  public void testRun() throws IOException {
    indexDocuments(document1, document2, document3, document4);

    lucene2seq.run(lucene2SeqConf);


    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator();

    assertSimpleDocumentEquals(document1, iterator.next());
    assertSimpleDocumentEquals(document2, iterator.next());
    assertSimpleDocumentEquals(document3, iterator.next());
    assertSimpleDocumentEquals(document4, iterator.next());
  }

  private Iterator<Pair<Text, Text>> getSequenceFileIterator() {
    Path sequenceFilesOutputPath = lucene2SeqConf.getSequenceFilesOutputPath();
    Configuration configuration = lucene2SeqConf.getConfiguration();
    return new SequenceFileDirIterable<Text, Text>(sequenceFilesOutputPath, PathType.LIST, configuration).iterator();
  }

  private void assertSimpleDocumentEquals(SingleFieldDocument expected, Pair<Text, Text> actual) {
    assertEquals(expected.getId(), actual.getFirst().toString());
    assertEquals(expected.getField(), actual.getSecond().toString());
  }

  private void indexDocuments(SingleFieldDocument... documents) throws IOException {
    IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(index.toString())), new IndexWriterConfig(Version.LUCENE_31, new DefaultAnalyzer()));

    for (SingleFieldDocument singleFieldDocument : documents) {
      indexWriter.addDocument(singleFieldDocument.asLuceneDocument());
    }

    indexWriter.commit();
    indexWriter.close();
  }
}
