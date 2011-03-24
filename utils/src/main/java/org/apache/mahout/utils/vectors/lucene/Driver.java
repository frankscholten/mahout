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

package org.apache.mahout.utils.vectors.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.vectorizer.TF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Driver {
  private static final Logger log = LoggerFactory.getLogger(Driver.class);

  private Driver() { }

  public static void main(String[] args) throws IOException {

    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();

    Option inputOpt = obuilder.withLongName("dir").withRequired(true).withArgument(
      abuilder.withName("dir").withMinimum(1).withMaximum(1).create())
        .withDescription("The Lucene directory").withShortName("d").create();

    Option outputOpt = obuilder.withLongName("output").withRequired(true).withArgument(
      abuilder.withName("output").withMinimum(1).withMaximum(1).create()).withDescription("The output file")
        .withShortName("o").create();

    Option fieldOpt = obuilder.withLongName("field").withRequired(true).withArgument(
      abuilder.withName("field").withMinimum(1).withMaximum(1).create()).withDescription(
      "The field in the index").withShortName("f").create();

    Option idFieldOpt = obuilder.withLongName("idField").withRequired(false).withArgument(
      abuilder.withName("idField").withMinimum(1).withMaximum(1).create()).withDescription(
      "The field in the index containing the index.  If null, then the Lucene internal doc "
          + "id is used which is prone to error if the underlying index changes").withShortName("i").create();

    Option dictOutOpt = obuilder.withLongName("dictOut").withRequired(true).withArgument(
      abuilder.withName("dictOut").withMinimum(1).withMaximum(1).create()).withDescription(
      "The output of the dictionary").withShortName("t").create();

    Option weightOpt = obuilder.withLongName("weight").withRequired(false).withArgument(
      abuilder.withName("weight").withMinimum(1).withMaximum(1).create()).withDescription(
      "The kind of weight to use. Currently TF or TFIDF").withShortName("w").create();

    Option delimiterOpt = obuilder.withLongName("delimiter").withRequired(false).withArgument(
      abuilder.withName("delimiter").withMinimum(1).withMaximum(1).create()).withDescription(
      "The delimiter for outputting the dictionary").withShortName("l").create();

    Option powerOpt = obuilder.withLongName("norm").withRequired(false).withArgument(
      abuilder.withName("norm").withMinimum(1).withMaximum(1).create()).withDescription(
      "The norm to use, expressed as either a double or \"INF\" if you want to use the Infinite norm.  "
          + "Must be greater or equal to 0.  The default is not to normalize").withShortName("n").create();

    Option maxOpt = obuilder.withLongName("max").withRequired(false).withArgument(
      abuilder.withName("max").withMinimum(1).withMaximum(1).create()).withDescription(
      "The maximum number of vectors to output.  If not specified, then it will loop over all docs")
        .withShortName("m").create();

    Option outWriterOpt = obuilder.withLongName("outputWriter").withRequired(false).withArgument(
      abuilder.withName("outputWriter").withMinimum(1).withMaximum(1).create()).withDescription(
      "The VectorWriter to use, either seq "
          + "(SequenceFileVectorWriter - default) or file (Writes to a File using JSON format)")
        .withShortName("e").create();

    Option minDFOpt = obuilder.withLongName("minDF").withRequired(false).withArgument(
      abuilder.withName("minDF").withMinimum(1).withMaximum(1).create()).withDescription(
      "The minimum document frequency.  Default is " + LuceneConverterConfiguration.DEFAULT_MIN_DF).withShortName("md").create();

    Option maxDFPercentOpt = obuilder.withLongName("maxDFPercent").withRequired(false).withArgument(
      abuilder.withName("maxDFPercent").withMinimum(1).withMaximum(1).create()).withDescription(
      "The max percentage of docs for the DF.  Can be used to remove really high frequency terms."
          + "  Expressed as an integer between 0 and 100. Default is " + LuceneConverterConfiguration.DEFAULT_MAX_DF_PERCENTAGE).withShortName("x").create();

    Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
        .create();

    Group group = gbuilder.withName("Options").withOption(inputOpt).withOption(idFieldOpt).withOption(
      outputOpt).withOption(delimiterOpt).withOption(helpOpt).withOption(fieldOpt).withOption(maxOpt)
        .withOption(dictOutOpt).withOption(powerOpt).withOption(outWriterOpt).withOption(maxDFPercentOpt)
        .withOption(weightOpt).withOption(minDFOpt).create();

    try {
      Parser parser = new Parser();
      parser.setGroup(group);
      CommandLine cmdLine = parser.parse(args);

      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return;
      }

      if (cmdLine.hasOption(inputOpt)) { // Lucene case

        File file = new File(cmdLine.getValue(inputOpt).toString());
        if (!file.isDirectory()) {
          throw new IllegalArgumentException("Lucene directory: " + file.getAbsolutePath()
              +  " does not exist or is not a directory");
        }

        String outFile = cmdLine.getValue(outputOpt).toString();
        log.info("Output File: {}", outFile);

        String field = cmdLine.getValue(fieldOpt).toString();

        LuceneConverterConfiguration luceneConfiguration = new LuceneConverterConfiguration(file, new Path(outFile), field);

        if (cmdLine.hasOption(weightOpt)) {
          String wString = cmdLine.getValue(weightOpt).toString();
          if ("tf".equalsIgnoreCase(wString)) {
            luceneConfiguration.setWeight(new TF());
          } else {
            throw new OptionException(weightOpt);
          }
        }

        if (cmdLine.hasOption(minDFOpt)) {
          luceneConfiguration.setMinDf(Integer.parseInt(cmdLine.getValue(minDFOpt).toString()));
        }

        if (cmdLine.hasOption(maxDFPercentOpt)) {
          luceneConfiguration.setMaxDfPercentage(Integer.parseInt(cmdLine.getValue(maxDFPercentOpt).toString()));
        }

        if (cmdLine.hasOption(powerOpt)) {
          String power = cmdLine.getValue(powerOpt).toString();
          if ("INF".equals(power)) {
            luceneConfiguration.setNormPower(Double.POSITIVE_INFINITY);
          } else {
            luceneConfiguration.setNormPower(Double.parseDouble(power));
          }
        }

        if (cmdLine.hasOption(idFieldOpt)) {
          luceneConfiguration.setIdField(cmdLine.getValue(idFieldOpt).toString());
        }

        if (cmdLine.hasOption(outWriterOpt)) {
          String outWriter = cmdLine.getValue(outWriterOpt).toString();
          if ("file".equals(outWriter)) {
            luceneConfiguration.useSeqFileWriter();
          } else {
            luceneConfiguration.useJsonVectorWriter();
          }
        }

        if (cmdLine.hasOption(delimiterOpt)) {
          luceneConfiguration.setDelimiter(cmdLine.getValue(delimiterOpt).toString());
        }

        luceneConfiguration.setOutputDictionary(new File(cmdLine.getValue(dictOutOpt).toString()));

        if (cmdLine.hasOption(maxOpt)) {
          long maxDocs = Long.parseLong(cmdLine.getValue(maxOpt).toString());
          if (maxDocs < 0) {
            throw new IllegalArgumentException("maxDocs must be >= 0");
          }
          luceneConfiguration.setMaxVectors(maxDocs);
        }

        new LuceneConverter().convertLuceneVectors(luceneConfiguration);
      }
    } catch (OptionException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    }
  }
}
