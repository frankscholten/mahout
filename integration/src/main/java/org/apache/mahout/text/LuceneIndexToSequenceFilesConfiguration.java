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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Holds all the configuration for {@link org.apache.mahout.text.LuceneIndexToSequenceFiles}, which generates a sequence file
 * with id as the key and a content field as value.
 */
public class LuceneIndexToSequenceFilesConfiguration implements Writable {

  private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();
  private static final int DEFAULT_MAX_HITS = Integer.MAX_VALUE;

  private static final String SERIALIZATION_KEY = "org.apache.mahout.text.LuceneIndexToSequenceFiles";
  
  static final String SEPARATOR_FIELDS = ",";
  static final String SEPARATOR_PATHS = ",";

  private Configuration configuration;
  private List<Path> indexPaths;
  private Path sequenceFilesOutputPath;
  private String idField;
  private List<String> fields;
  private Query query;
  private int maxHits;

  /**
   * Create a configuration bean with all mandatory parameters.
   *
   * @param configuration           Hadoop configuration for writing sequencefiles
   * @param indexPaths              paths to the index
   * @param sequenceFilesOutputPath path to output the sequence file
   * @param idField                 field used for the key of the sequence file
   * @param fields                  field(s) used for the value of the sequence file
   */
  public LuceneIndexToSequenceFilesConfiguration(Configuration configuration, List<Path> indexPaths, Path sequenceFilesOutputPath, String idField, List<String> fields) {
    Preconditions.checkArgument(configuration != null, "Parameter 'configuration' cannot be null");
    Preconditions.checkArgument(indexPaths != null, "Parameter 'indexPaths' cannot be null");
    Preconditions.checkArgument(indexPaths != null && !indexPaths.isEmpty(), "Parameter 'indexPaths' cannot be empty");
    Preconditions.checkArgument(sequenceFilesOutputPath != null, "Parameter 'sequenceFilesOutputPath' cannot be null");
    Preconditions.checkArgument(idField != null, "Parameter 'idField' cannot be null");
    Preconditions.checkArgument(fields != null, "Parameter 'fields' cannot be null");
    Preconditions.checkArgument(fields != null && !fields.isEmpty(), "Parameter 'fields' cannot be empty");

    this.configuration = configuration;
    this.indexPaths = indexPaths;
    this.sequenceFilesOutputPath = sequenceFilesOutputPath;
    this.idField = idField;
    this.fields = fields;

    setQuery(DEFAULT_QUERY);
    setMaxHits(DEFAULT_MAX_HITS);
  }

  public LuceneIndexToSequenceFilesConfiguration() {
    // For deserialization
  }

  public LuceneIndexToSequenceFilesConfiguration getFromConfiguration(Configuration configuration) throws IOException {
    String serializedConfigString = configuration.get(LuceneIndexToSequenceFilesConfiguration.SERIALIZATION_KEY);
    return new DefaultStringifier<LuceneIndexToSequenceFilesConfiguration>(configuration, LuceneIndexToSequenceFilesConfiguration.class).fromString(serializedConfigString);
  }

  public Configuration serializeInConfiguration() throws IOException {
    String serializedConfigString = new DefaultStringifier<LuceneIndexToSequenceFilesConfiguration>(configuration, LuceneIndexToSequenceFilesConfiguration.class).toString(this);

    Configuration serializedConfig = new Configuration(configuration);
    serializedConfig.set(SERIALIZATION_KEY, serializedConfigString);

    return serializedConfig;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getSequenceFilesOutputPath() {
    return sequenceFilesOutputPath;
  }

  public List<Path> getIndexPaths() {
    return indexPaths;
  }

  public String getIdField() {
    return idField;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  public Query getQuery() {
    return query;
  }

  public void setMaxHits(int maxHits) {
    this.maxHits = maxHits;
  }

  public int getMaxHits() {
    return maxHits;
  }

  public FieldSelector getFieldSelector() {
    Set<String> fieldSet = Sets.newHashSet(idField);
    fieldSet.addAll(fields);
    return new SetBasedFieldSelector(fieldSet, Collections.<String>emptySet());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(sequenceFilesOutputPath.toString());
    out.writeUTF(StringUtils.join(indexPaths, SEPARATOR_PATHS));
    out.writeUTF(idField);
    out.writeUTF(StringUtils.join(fields, SEPARATOR_FIELDS));
    out.writeUTF(query.toString());
    out.writeInt(maxHits);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      this.sequenceFilesOutputPath = new Path(in.readUTF());
      this.indexPaths = new ArrayList<Path>();
      String[] indexPaths = in.readUTF().split(SEPARATOR_PATHS);
      for (String indexPath : indexPaths) {
        this.indexPaths.add(new Path(indexPath));
      }
      this.idField = in.readUTF();
      this.fields = Arrays.asList(in.readUTF().split(SEPARATOR_FIELDS));
      this.query = new QueryParser(Version.LUCENE_35, "query", new StandardAnalyzer(Version.LUCENE_35)).parse(in.readUTF());
      this.maxHits = in.readInt();
    } catch (ParseException e) {
      throw new RuntimeException("Could not deserialize " + this.getClass().getName(), e);
    }
  }
}
