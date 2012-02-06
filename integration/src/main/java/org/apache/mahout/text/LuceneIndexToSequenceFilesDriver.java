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
import org.apache.mahout.common.AbstractJob;
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

    public static final String OPTION_EXTRA_FIELDS = "extraFields";
    public static final String OPTION_ID_FIELD = "idField";

    private static final String DEFAULT_ID_FIELD = "id";

    private static final String SEPARATOR_EXTRA_FIELDS = ",";

    private static final Logger log = LoggerFactory.getLogger(LuceneIndexToSequenceFilesDriver.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new LuceneIndexToSequenceFilesDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            LuceneIndexToSequenceFilesConfiguration lucene2SeqConf = createConfguration(args);
            new LuceneIndexToSequenceFiles().run(lucene2SeqConf);
        } catch (OptionException e) {
            log.error("Exception", e);
//            CommandLineUtil.printHelp(group);
        }
        return 0;
    }

    private LuceneIndexToSequenceFilesConfiguration createConfguration(String[] args) throws OptionException {
        DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
        ArgumentBuilder abuilder = new ArgumentBuilder();
        GroupBuilder gbuilder = new GroupBuilder();

        Option inputOpt = obuilder.withLongName("dir").withRequired(true).withArgument(
                abuilder.withName("dir").withMinimum(1).withMaximum(1).create())
                .withDescription("The Lucene directory").withShortName("d").create();

        Option outputDirOpt = DefaultOptionCreator.outputOption().create();

        Option idFieldOpt = obuilder.withLongName(OPTION_ID_FIELD).withRequired(false).withArgument(
                abuilder.withName(OPTION_ID_FIELD).withMinimum(1).withMaximum(1).create()).withShortName("i").withDescription(
                "The field in the index containing the index.  If null, then the Lucene internal doc "
                        + "id is used which is prone to error if the underlying index changes").create();

        Option fieldOpt = obuilder.withLongName("field").withRequired(true).withArgument(
                abuilder.withName("field").withMinimum(1).withMaximum(1).create()).withDescription(
                "The field in the index").withShortName("f").create();

        Option extraFieldsOpt = obuilder.withLongName(OPTION_EXTRA_FIELDS).withRequired(false).withArgument(
                abuilder.withName(OPTION_EXTRA_FIELDS).withMinimum(1).withMaximum(1).create()).withDescription(
                "(Optional) Extra stored fields. Comma separated").withShortName("e").create();

        Group group = gbuilder.withName("Options").withOption(inputOpt).withOption(outputDirOpt)
                              .withOption(idFieldOpt).withOption(fieldOpt).withOption(extraFieldsOpt).create();

        Parser parser = new Parser();
        parser.setGroup(group);
        CommandLine cmdLine = parser.parse(args);

        Configuration configuration = getConf();
        if (configuration == null) {
            configuration = new Configuration();
        }

        String indexLocation = ((String) cmdLine.getValue(inputOpt));
        Path sequenceFilesOutputPath = new Path((String) cmdLine.getValue(outputDirOpt));

        String idField = DEFAULT_ID_FIELD;
        if (cmdLine.hasOption(idFieldOpt)) {
            idField = (String) cmdLine.getValue(idFieldOpt);
        }

        String field = (String) cmdLine.getValue(fieldOpt);

        LuceneIndexToSequenceFilesConfiguration lucene2SeqConf = newLucene2SeqConfiguration(configuration, indexLocation, sequenceFilesOutputPath, idField, field);

        if (cmdLine.hasOption(extraFieldsOpt)) {
            String extraFields = (String) cmdLine.getValue(extraFieldsOpt);
            lucene2SeqConf.setExtraFields(asList(extraFields.split(SEPARATOR_EXTRA_FIELDS)));
        }

        return lucene2SeqConf;
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
