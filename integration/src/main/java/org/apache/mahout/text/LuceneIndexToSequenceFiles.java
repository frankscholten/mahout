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

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Generates a sequence file from a Lucene index with a specified id field as the key and a content field as the value.
 * Configure this class with a {@link LuceneIndexToSequenceFilesConfiguration} bean.
 */
public class LuceneIndexToSequenceFiles {

    public static final String FIELD_SEPARATOR = " ";

    private static final Logger log = LoggerFactory.getLogger(LuceneIndexToSequenceFiles.class);

    /**
     * Generates a sequence files from a Lucene index via the given {@link LuceneIndexToSequenceFilesConfiguration}
     *
     * @param lucene2seqConf configuration bean
     * @throws java.io.IOException if index cannot be opened or sequence file could not be written
     */
    public void run(LuceneIndexToSequenceFilesConfiguration lucene2seqConf) throws IOException {
        Directory directory = FSDirectory.open(lucene2seqConf.getIndexLocation());
        IndexReader reader = IndexReader.open(directory, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        Configuration configuration = lucene2seqConf.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        SequenceFile.Writer sequenceFileWriter = new SequenceFile.Writer(fileSystem, configuration, lucene2seqConf.getSequenceFilesOutputPath(), Text.class, Text.class);

        Text key = new Text();
        Text value = new Text();

        Weight weight = lucene2seqConf.getQuery().createWeight(searcher);
        Scorer scorer = weight.scorer(reader, true, false);

        if (scorer != null) {
            int processedDocs = 0;
            int docId;
            
            while ((docId = scorer.nextDoc()) != NO_MORE_DOCS && processedDocs < lucene2seqConf.getMaxHits()) {
                Document doc = reader.document(docId, lucene2seqConf.getFieldSelector());

                String idValue = doc.get(lucene2seqConf.getIdField());
                String field = lucene2seqConf.getField();
                String fieldValue = doc.get(field);

                if (fieldValue == null) {
                    continue;
                }

                StringBuilder fieldValueBuilder = new StringBuilder(fieldValue);
                if (lucene2seqConf.getExtraFields() != null && !lucene2seqConf.getExtraFields().isEmpty()) {
                    for (String extraField : lucene2seqConf.getExtraFields()) {
                        String extraFieldValue = doc.get(extraField);
                        if (!isBlank(extraFieldValue)) {
                            fieldValueBuilder.append(FIELD_SEPARATOR).append(extraFieldValue);
                        }
                    }
                }

                String concatenatedFieldValue = fieldValueBuilder.toString();

                if (isBlank(idValue) || isBlank(concatenatedFieldValue)) {
                    continue;
                }

                key.set(idValue);
                value.set(concatenatedFieldValue);

                sequenceFileWriter.append(key, value);

                processedDocs++;
            }

            log.info("Wrote " + processedDocs + " documents");
        } else {
            Closeables.closeQuietly(sequenceFileWriter);
            directory.close();
            searcher.close();
            reader.close();
            throw new RuntimeException("Could not write sequence files. Could not create scorer");
        }

        Closeables.closeQuietly(sequenceFileWriter);
        directory.close();
        searcher.close();
        reader.close();
    }

    private boolean isBlank(String idValue) {
        return idValue == null || idValue.equals("");
    }
}
