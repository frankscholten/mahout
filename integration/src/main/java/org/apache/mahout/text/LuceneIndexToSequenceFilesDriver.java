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


import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static java.util.Arrays.asList;

/**
 * Driver class for the lucene2seq program. Converts text contents of stored fields of a lucene index into a Hadoop
 * SequenceFile. The key of the sequence file is the document ID and the value is the concatenated text of the specified
 * field and any extra fields.
 */
public class LuceneIndexToSequenceFilesDriver extends AbstractJob {

  static final String OPTION_LUCENE_DIRECTORY = "dir";
  static final String OPTION_EXTRA_FIELDS = "extraFields";
  static final String OPTION_ID_FIELD = "idField";
  static final String OPTION_QUERY = "query";
  static final String OPTION_MAX_HITS = "maxHits";

  static final Query DEFAULT_QUERY = new MatchAllDocsQuery();
  static final int DEFAULT_MAX_HITS = Integer.MAX_VALUE;

  static final String SEPARATOR_EXTRA_FIELDS = ",";
  static final String QUERY_DELIMITER = "'";

  private static final Logger log = LoggerFactory.getLogger(LuceneIndexToSequenceFilesDriver.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LuceneIndexToSequenceFilesDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();

    Option inputOpt = obuilder.withLongName(OPTION_LUCENE_DIRECTORY).withRequired(true).withArgument(
      abuilder.withName(OPTION_LUCENE_DIRECTORY).withMinimum(1).withMaximum(1).create())
      .withDescription("The Lucene directory").withShortName("d").create();

    Option outputDirOpt = DefaultOptionCreator.outputOption().create();

    Option idFieldOpt = obuilder.withLongName(OPTION_ID_FIELD).withRequired(true).withArgument(
      abuilder.withName(OPTION_ID_FIELD).withMinimum(1).withMaximum(1).create()).withShortName("i").withDescription(
      "The field in the index containing the id").create();

    Option fieldOpt = obuilder.withLongName("field").withRequired(true).withArgument(
      abuilder.withName("field").withMinimum(1).withMaximum(1).create()).withDescription(
      "The stored field in the index containing text").withShortName("f").create();

    Option extraFieldsOpt = obuilder.withLongName(OPTION_EXTRA_FIELDS).withRequired(false).withArgument(
      abuilder.withName(OPTION_EXTRA_FIELDS).withMinimum(1).withMaximum(1).create()).withDescription(
      "(Optional) Extra stored fields. Comma separated").withShortName("e").create();

    Option queryOpt = obuilder.withLongName(OPTION_QUERY).withRequired(false).withArgument(
      abuilder.withName(OPTION_QUERY).withMinimum(1).withMaximum(1).create()).withDescription(
      "(Optional) Lucene query. Defaults to " + DEFAULT_QUERY.getClass().getSimpleName()).withShortName("q").create();

    Option maxHitsOpt = obuilder.withLongName(OPTION_MAX_HITS).withRequired(false).withArgument(
      abuilder.withName(OPTION_MAX_HITS).withMinimum(1).withMaximum(1).create()).withDescription(
      "(Optional) Max hits. Defaults to " + DEFAULT_MAX_HITS).withShortName("n").create();

    Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
      .create();

    Group group = gbuilder.withName("Options").withOption(inputOpt).withOption(outputDirOpt)
      .withOption(idFieldOpt).withOption(fieldOpt).withOption(extraFieldsOpt)
      .withOption(queryOpt).withOption(maxHitsOpt).withOption(helpOpt).create();

    try {
      Parser parser = new Parser();
      parser.setGroup(group);
      parser.setHelpOption(helpOpt);
      CommandLine cmdLine = parser.parse(args);

      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return -1;
      }

      Configuration configuration = getConf();
      if (configuration == null) {
        configuration = new Configuration();
      }

      String indexLocation = ((String) cmdLine.getValue(inputOpt));
      Path sequenceFilesOutputPath = new Path((String) cmdLine.getValue(outputDirOpt));

      String idField = (String) cmdLine.getValue(idFieldOpt);
      String field = (String) cmdLine.getValue(fieldOpt);

      LuceneIndexToSequenceFilesConfiguration lucene2SeqConf = newLucene2SeqConfiguration(configuration,
        indexLocation,
        sequenceFilesOutputPath,
        idField,
        field);

      if (cmdLine.hasOption(extraFieldsOpt)) {
        String extraFields = (String) cmdLine.getValue(extraFieldsOpt);
        lucene2SeqConf.setExtraFields(asList(extraFields.split(SEPARATOR_EXTRA_FIELDS)));
      }

      Query query = DEFAULT_QUERY;
      if (cmdLine.hasOption(queryOpt)) {
        try {
          String queryString = ((String) cmdLine.getValue(queryOpt)).replaceAll(QUERY_DELIMITER, "");
          QueryParser queryParser = new QueryParser(Version.LUCENE_35, queryString, new StandardAnalyzer(Version.LUCENE_35));
          query = queryParser.parse(queryString);
        } catch (ParseException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }
      lucene2SeqConf.setQuery(query);

      int maxHits = DEFAULT_MAX_HITS;
      if (cmdLine.hasOption(maxHitsOpt)) {
        String maxHitsString = (String) cmdLine.getValue(maxHitsOpt);
        maxHits = Integer.valueOf(maxHitsString);
      }
      lucene2SeqConf.setMaxHits(maxHits);

      new LuceneIndexToSequenceFiles().run(lucene2SeqConf);
    } catch (OptionException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    }
    return 0;
  }

  public LuceneIndexToSequenceFilesConfiguration newLucene2SeqConfiguration(Configuration configuration,
                                                                            String indexLocation,
                                                                            Path sequenceFilesOutputPath,
                                                                            String idField,
                                                                            String field) {
    return new LuceneIndexToSequenceFilesConfiguration(
      configuration,
      new File(indexLocation),
      sequenceFilesOutputPath,
      idField,
      field);
  }
}
