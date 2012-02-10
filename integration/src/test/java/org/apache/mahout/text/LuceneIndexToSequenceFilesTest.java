package org.apache.mahout.text;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
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

public class LuceneIndexToSequenceFilesTest {

  private LuceneIndexToSequenceFiles lucene2Seq;
  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConf;

  private File indexLocation;

  private SimpleDocument document1;
  private SimpleDocument document2;
  private SimpleDocument document3;

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws IOException {
    Configuration configuration = new Configuration();
    indexLocation = new File("/tmp", getClass().getSimpleName());
    Path seqFilesOutputPath = new Path("seqfiles");

    lucene2Seq = new LuceneIndexToSequenceFiles();
    lucene2SeqConf = new LuceneIndexToSequenceFilesConfiguration(configuration,
      indexLocation,
      seqFilesOutputPath,
      SimpleDocument.ID_FIELD,
      SimpleDocument.FIELD);

    document1 = new SimpleDocument("1", "This is test document 1");
    document2 = new SimpleDocument("2", "This is test document 2");
    document3 = new SimpleDocument("3", "This is test document 3");
  }

  @After
  public void after() throws IOException {
    HadoopUtil.delete(lucene2SeqConf.getConfiguration(), lucene2SeqConf.getSequenceFilesOutputPath());
    FileUtils.deleteDirectory(lucene2SeqConf.getIndexLocation());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun() throws Exception {
    indexDocuments(document1, document2, document3);

    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertSimpleDocumentEquals(document1, iterator.next());
    assertSimpleDocumentEquals(document2, iterator.next());
    assertSimpleDocumentEquals(document3, iterator.next());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun_skipEmptyIdFieldDocs() throws IOException {
    indexDocuments(document1, new SimpleDocument("", "This is a test document with no id"), document2);

    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertSimpleDocumentEquals(document1, iterator.next());
    assertSimpleDocumentEquals(document2, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun_skipEmptyFieldDocs() throws IOException {
    indexDocuments(document1, new SimpleDocument("4", ""), document2);

    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertSimpleDocumentEquals(document1, iterator.next());
    assertSimpleDocumentEquals(document2, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun_skipUnstoredFields() throws IOException {
    indexDocuments(new UnstoredFieldsDocument("5", "This is test document 5"));

    lucene2SeqConf.setExtraFields(asList(UnstoredFieldsDocument.UNSTORED_FIELD));

    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertFalse(iterator.next().getSecond().toString().contains("null"));
    assertFalse(iterator.hasNext());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun_maxHits() throws IOException {
    indexDocuments(document1, document2, document3, new SimpleDocument("4", "This is test document 4"));

    lucene2SeqConf.setMaxHits(3);
    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertSimpleDocumentEquals(document1, iterator.next());
    assertSimpleDocumentEquals(document2, iterator.next());
    assertSimpleDocumentEquals(document3, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRun_query() throws IOException {
    indexDocuments(document1, document2, document3, new SimpleDocument("4", "Mahout is cool"));

    Query query = new TermQuery(new Term(lucene2SeqConf.getField(), "mahout"));

    lucene2SeqConf.setQuery(query);
    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertSimpleDocumentEquals(new SimpleDocument("4", "Mahout is cool"), iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testRun_extraFields() throws IOException {
    ExtraFieldsDocument extraFieldsDocument1 = new ExtraFieldsDocument("1", "This is test document 1", "This is extrafield1 1", "This is extrafield2 1");
    ExtraFieldsDocument extraFieldsDocument2 = new ExtraFieldsDocument("2", "This is test document 2", "This is extrafield1 2", "This is extrafield2 1");
    ExtraFieldsDocument extraFieldsDocument3 = new ExtraFieldsDocument("3", "This is test document 3", "This is extrafield1 3", "This is extrafield3 1");
    indexDocuments(extraFieldsDocument1, extraFieldsDocument2, extraFieldsDocument3);

    lucene2SeqConf.setExtraFields(asList(ExtraFieldsDocument.EXTRA_FIELD1, ExtraFieldsDocument.EXTRA_FIELD2));
    lucene2Seq.run(lucene2SeqConf);

    Iterator<Pair<Text, Text>> iterator = getSequenceFileIterator(lucene2SeqConf);

    assertExtraFieldsDocumentEquals(extraFieldsDocument1, iterator.next());
    assertExtraFieldsDocumentEquals(extraFieldsDocument2, iterator.next());
    assertExtraFieldsDocumentEquals(extraFieldsDocument3, iterator.next());
  }

  private void indexDocuments(SimpleDocument... documents) throws IOException {
    IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexLocation), new IndexWriterConfig(Version.LUCENE_31, new DefaultAnalyzer()));

    for (SimpleDocument simpleDocument : documents) {
      indexWriter.addDocument(simpleDocument.asLuceneDocument());
    }

    indexWriter.commit();
    indexWriter.close();
  }

  private Iterator<Pair<Text, Text>> getSequenceFileIterator(LuceneIndexToSequenceFilesConfiguration lucene2SeqConf) {
    Path sequenceFilesOutputPath = lucene2SeqConf.getSequenceFilesOutputPath();
    Configuration configuration = lucene2SeqConf.getConfiguration();
    return new SequenceFileIterable<Text, Text>(sequenceFilesOutputPath, true, configuration).iterator();
  }

  private void assertSimpleDocumentEquals(SimpleDocument expected, Pair<Text, Text> actual) {
    assertEquals(expected.getId(), actual.getFirst().toString());
    assertEquals(expected.getField(), actual.getSecond().toString());
  }

  private void assertExtraFieldsDocumentEquals(ExtraFieldsDocument expected, Pair<Text, Text> actual) {
    assertEquals(expected.getId(), actual.getFirst().toString());
    assertEquals(expected.getField() + " " + expected.getExtraField1() + " " + expected.getExtraField2(), actual.getSecond().toString());
  }

  private static class SimpleDocument {

    public static final String ID_FIELD = "idField";
    public static final String FIELD = "field";

    private String id;
    private String field;

    SimpleDocument(String id, String field) {
      this.id = id;
      this.field = field;
    }

    public String getId() {
      return id;
    }

    public String getField() {
      return field;
    }

    protected Document asLuceneDocument() {
      Document document = new Document();

      Field idField = new Field(ID_FIELD, getId(), Field.Store.YES, Field.Index.NO);
      Field field = new Field(FIELD, getField(), Field.Store.YES, Field.Index.ANALYZED);

      document.add(idField);
      document.add(field);

      return document;
    }
  }

  private static class ExtraFieldsDocument extends SimpleDocument {

    public static final String EXTRA_FIELD1 = "extraField1";
    public static final String EXTRA_FIELD2 = "extraField2";

    private String extraField1;
    private String extraField2;

    ExtraFieldsDocument(String id, String field, String extraField1, String extraField2) {
      super(id, field);
      this.extraField1 = extraField1;
      this.extraField2 = extraField2;
    }

    public String getExtraField1() {
      return extraField1;
    }

    public String getExtraField2() {
      return extraField2;
    }

    @Override
    public Document asLuceneDocument() {
      Document document = super.asLuceneDocument();

      Field extraField1 = new Field(EXTRA_FIELD1, this.extraField1, Field.Store.YES, Field.Index.ANALYZED);
      Field extraField2 = new Field(EXTRA_FIELD2, this.extraField2, Field.Store.YES, Field.Index.ANALYZED);

      document.add(extraField1);
      document.add(extraField2);

      return document;
    }
  }

  private class UnstoredFieldsDocument extends SimpleDocument {

    public static final String UNSTORED_FIELD = "unstored";

    UnstoredFieldsDocument(String id, String field) {
      super(id, field);
    }

    @Override
    public Document asLuceneDocument() {
      Document document = super.asLuceneDocument();

      Field unstoredField = new Field(UNSTORED_FIELD, "", Field.Store.NO, Field.Index.NOT_ANALYZED);

      document.add(unstoredField);

      return document;
    }
  }
}
