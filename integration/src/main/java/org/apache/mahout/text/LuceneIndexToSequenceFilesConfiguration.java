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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Holds all the configuration for {@link org.apache.mahout.text.LuceneIndexToSequenceFiles}, which generates a sequence file
 * with id as the key and a content field as value.
 */
public class LuceneIndexToSequenceFilesConfiguration implements Writable {

  private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();
  private static final int DEFAULT_MAX_HITS = Integer.MAX_VALUE;
  @SuppressWarnings("unchecked")
  private static final List<String> DEFAULT_EMPTY_FIELDS = Collections.EMPTY_LIST;

  private static final String SERIALIZATION_KEY = "org.apache.mahout.text.LuceneIndexToSequenceFiles";
  public static final String SEPARATOR_EXTRA_FIELDS = ",";

  private Configuration configuration;
  private Path indexLocation;
  private Path sequenceFilesOutputPath;
  private String idField;
  private String field;
  private List<String> extraFields;
  private Query query;
  private int maxHits;

  /**
   * Create a configuration bean with all mandatory parameters.
   *
   * @param configuration           Hadoop configuration for writing sequencefiles
   * @param index                   path to the index
   * @param sequenceFilesOutputPath path to output the sequence file
   * @param idField                 field used for the key of the sequence file
   * @param field                   field used for the value of the sequence file
   */
  public LuceneIndexToSequenceFilesConfiguration(Configuration configuration, Path index, Path sequenceFilesOutputPath, String idField, String field) {
    Preconditions.checkArgument(configuration != null, "Parameter 'configuration' cannot be null");
    Preconditions.checkArgument(index != null, "Parameter 'index' cannot be null");
    Preconditions.checkArgument(sequenceFilesOutputPath != null, "Parameter 'sequenceFilesOutputPath' cannot be null");
    Preconditions.checkArgument(idField != null, "Parameter 'idField' cannot be null");
    Preconditions.checkArgument(field != null, "Parameter 'field' cannot be null");

    this.configuration = configuration;
    this.indexLocation = index;
    this.sequenceFilesOutputPath = sequenceFilesOutputPath;
    this.idField = idField;
    this.field = field;

    setExtraFields(DEFAULT_EMPTY_FIELDS);
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

  public Path getIndexPath() {
    return indexLocation;
  }

  public String getIdField() {
    return idField;
  }

  public String getField() {
    return field;
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

  public List<String> getExtraFields() {
    return extraFields;
  }

  public void setExtraFields(List<String> extraFields) {
    this.extraFields = extraFields;
  }

  public FieldSelector getFieldSelector() {
    Set<String> fieldSet = Sets.newHashSet(idField, field);
    if (extraFields != null) {
      fieldSet.addAll(extraFields);
    }
    return new SetBasedFieldSelector(fieldSet, Collections.<String>emptySet());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(sequenceFilesOutputPath.toString());
    out.writeUTF(indexLocation.toString());
    out.writeUTF(idField);
    out.writeUTF(field);
    out.writeUTF(query.toString());
    out.writeInt(maxHits);
    out.writeUTF(StringUtils.join(extraFields, SEPARATOR_EXTRA_FIELDS));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      this.sequenceFilesOutputPath = new Path(in.readUTF());
      this.indexLocation = new Path(in.readUTF());
      this.idField = in.readUTF();
      this.field = in.readUTF();
      this.query = new QueryParser(Version.LUCENE_35, "query", new StandardAnalyzer(Version.LUCENE_35)).parse(in.readUTF());
      this.maxHits = in.readInt();
      this.extraFields = Arrays.asList(in.readUTF().split(SEPARATOR_EXTRA_FIELDS));
    } catch (ParseException e) {
      throw new RuntimeException("Could not deserialize " + this.getClass().getName(), e);
    }
  }
}
