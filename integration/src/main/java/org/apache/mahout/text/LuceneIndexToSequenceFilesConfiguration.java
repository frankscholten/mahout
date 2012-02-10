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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Holds all the configuration for {@link org.apache.mahout.text.LuceneIndexToSequenceFiles}, which generates a sequence file
 * with id as the key and a content field as value.
 */
public class LuceneIndexToSequenceFilesConfiguration {

  private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();
  private static final int DEFAULT_MAX_HITS = Integer.MAX_VALUE;
  @SuppressWarnings("unchecked")
  private static final List<String> DEFAULT_EMPTY_FIELDS = Collections.EMPTY_LIST;

  private Configuration configuration;
  private File indexLocation;
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
   * @param indexLocation           location of the index
   * @param sequenceFilesOutputPath path to output the sequence file
   * @param idField                 field used for the key of the sequence file
   * @param field                   field used for the value of the sequence file
   */
  public LuceneIndexToSequenceFilesConfiguration(Configuration configuration, File indexLocation, Path sequenceFilesOutputPath, String idField, String field) {
    Preconditions.checkArgument(configuration != null, "Parameter 'configuration' cannot be null");
    Preconditions.checkArgument(indexLocation != null, "Parameter 'indexLocation' cannot be null");
    Preconditions.checkArgument(sequenceFilesOutputPath != null, "Parameter 'sequenceFilesOutputPath' cannot be null");
    Preconditions.checkArgument(idField != null, "Parameter 'idField' cannot be null");
    Preconditions.checkArgument(field != null, "Parameter 'field' cannot be null");

    this.configuration = configuration;
    this.indexLocation = indexLocation;
    this.sequenceFilesOutputPath = sequenceFilesOutputPath;
    this.idField = idField;
    this.field = field;

    setExtraFields(DEFAULT_EMPTY_FIELDS);
    setQuery(DEFAULT_QUERY);
    setMaxHits(DEFAULT_MAX_HITS);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Path getSequenceFilesOutputPath() {
    return sequenceFilesOutputPath;
  }

  public File getIndexLocation() {
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
}
