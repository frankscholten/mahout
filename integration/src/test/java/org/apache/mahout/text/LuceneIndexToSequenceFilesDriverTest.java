/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.text;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import static java.util.Arrays.asList;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneIndexToSequenceFilesDriverTest {

  private LuceneIndexToSequenceFilesDriver driver;
  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConf;
  private File indexLocation;
  private String idField;
  private String field;
  private Path seqFilesOutputPath;
  private Configuration conf;

  @Before
  public void before() throws Exception {
    conf = new Configuration();
    indexLocation = new File("/tmp", getClass().getSimpleName());

    seqFilesOutputPath = new Path("seqfiles");
    idField = "id";
    field = "field";

    indexDocuments(new SimpleDocument("1", "Mahout is cool"));
  }

  @After
  public void after() throws IOException {
    HadoopUtil.delete(conf, seqFilesOutputPath);
    FileUtils.deleteDirectory(indexLocation);
  }

  @Test
  public void testNewLucene2SeqConfiguration() {
    driver = new LuceneIndexToSequenceFilesDriver();

    lucene2SeqConf = driver.newLucene2SeqConfiguration(conf,
      indexLocation.getAbsolutePath(),
      seqFilesOutputPath,
      idField,
      field);

    assertEquals(conf, lucene2SeqConf.getConfiguration());
    assertEquals(indexLocation, lucene2SeqConf.getIndexLocation());
    assertEquals(seqFilesOutputPath, lucene2SeqConf.getSequenceFilesOutputPath());
    assertEquals(idField, lucene2SeqConf.getIdField());
    assertEquals(field, lucene2SeqConf.getField());
  }

  @Test
  public void testRun() throws Exception {
    String queryField = "queryfield";
    String queryTerm = "queryterm";
    String maxHits = "500";
    String extraField1 = "extraField1";
    String extraField2 = "extraField2";

    String[] args = new String[]{
      "-d", indexLocation.getAbsolutePath(),
      "-o", seqFilesOutputPath.toString(),
      "-i", idField,
      "-f", field,
      "-q", queryField + ":" + queryTerm,
      "-n", maxHits,
      "-e", extraField1 + "," + extraField2,
    };

    stubLucene2SeqConfiguration(conf, indexLocation.getAbsolutePath(), seqFilesOutputPath, idField, field);

    driver.setConf(conf);
    driver.run(args);

    assertEquals(indexLocation, lucene2SeqConf.getIndexLocation());
    assertEquals(seqFilesOutputPath, lucene2SeqConf.getSequenceFilesOutputPath());
    assertEquals(idField, lucene2SeqConf.getIdField());
    assertEquals(field, lucene2SeqConf.getField());

    assertTrue(lucene2SeqConf.getExtraFields().containsAll(asList(extraField1, extraField2)));
    assertTrue(lucene2SeqConf.getQuery() instanceof TermQuery);
    assertEquals(queryField, ((TermQuery) lucene2SeqConf.getQuery()).getTerm().field());
    assertEquals(queryTerm, ((TermQuery) lucene2SeqConf.getQuery()).getTerm().text());
    assertEquals(new Integer(maxHits), (Integer) lucene2SeqConf.getMaxHits());
  }

  @Test
  public void testRun_optionalArguments() throws Exception {
    String[] args = new String[]{
      "-d", indexLocation.getAbsolutePath(),
      "-o", seqFilesOutputPath.toString(),
      "-i", idField,
      "-f", field
    };

    stubLucene2SeqConfiguration(conf, indexLocation.getAbsolutePath(), seqFilesOutputPath, idField, field);

    driver.setConf(conf);
    driver.run(args);

    assertEquals(indexLocation, lucene2SeqConf.getIndexLocation());
    assertEquals(seqFilesOutputPath, lucene2SeqConf.getSequenceFilesOutputPath());
    assertEquals(idField, lucene2SeqConf.getIdField());
    assertEquals(field, lucene2SeqConf.getField());
    assertEquals(conf, lucene2SeqConf.getConfiguration());

    assertTrue(lucene2SeqConf.getExtraFields().isEmpty());
    assertEquals(LuceneIndexToSequenceFilesDriver.DEFAULT_QUERY, lucene2SeqConf.getQuery());
    assertEquals(LuceneIndexToSequenceFilesDriver.DEFAULT_MAX_HITS, lucene2SeqConf.getMaxHits());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRun_invalidQuery() throws Exception {
    String[] args = new String[]{
      "-d", indexLocation.getAbsolutePath(),
      "-o", seqFilesOutputPath.toString(),
      "-i", idField,
      "-f", field,
      "-q", "inva:lid:query"
    };

    stubLucene2SeqConfiguration(conf, indexLocation.getAbsolutePath(), seqFilesOutputPath, idField, field);

    driver.setConf(conf);
    driver.run(args);
  }

  @Test
  public void testHelp() throws Exception {
    driver = new LuceneIndexToSequenceFilesDriver();
    driver.run(new String[]{"--help"});
  }

  //============================================ Helper Methods ========================================================

  private void stubLucene2SeqConfiguration(Configuration conf, String indexLocation, Path seqOutputPath, String idField, String field) throws NoSuchMethodException {
    Method method = LuceneIndexToSequenceFilesDriver.class.getMethod("newLucene2SeqConfiguration", Configuration.class, String.class, Path.class, String.class, String.class);

    driver = EasyMock.createMock(LuceneIndexToSequenceFilesDriver.class, method);

    lucene2SeqConf = new LuceneIndexToSequenceFilesConfiguration(conf, new File(indexLocation), seqOutputPath, idField, field);

    expect(driver.newLucene2SeqConfiguration(eq(conf), eq(indexLocation), eq(seqOutputPath), eq(idField), eq(field))).andReturn(lucene2SeqConf);
    replay(driver);
  }

  private static class SimpleDocument {
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
  }

  private void indexDocuments(SimpleDocument... documents) throws IOException {
    IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexLocation), new IndexWriterConfig(Version.LUCENE_31, new DefaultAnalyzer()));

    for (SimpleDocument simpleDocument : documents) {
      Document document = new Document();
      Field idField = new Field(this.idField, simpleDocument.getId(), Field.Store.YES, Field.Index.NO);
      Field field = new Field(this.field, simpleDocument.getField(), Field.Store.YES, Field.Index.ANALYZED);
      document.add(idField);
      document.add(field);
      indexWriter.addDocument(document);
    }
    indexWriter.commit();
    indexWriter.close();
  }
}
