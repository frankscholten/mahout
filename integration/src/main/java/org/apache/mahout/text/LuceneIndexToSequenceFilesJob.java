package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;

import static java.util.Arrays.asList;

/**
 * Generates a sequence file from a Lucene index via MapReduce. Uses a specified id field as the key and a content field as the value.
 * Configure this class with a {@link LuceneIndexToSequenceFilesConfiguration} bean.
 */
public class LuceneIndexToSequenceFilesJob {

  public void run(LuceneIndexToSequenceFilesConfiguration lucene2seqConf) {
    try {
      Configuration configuration = lucene2seqConf.serializeInConfiguration();

      Job job = new Job(configuration, "LuceneIndexToSequenceFiles: " + lucene2seqConf.getIndexPath() + " -> M/R -> " + lucene2seqConf.getSequenceFilesOutputPath());

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setMapperClass(LuceneIndexToSequenceFilesMapper.class);

      job.setInputFormatClass(LuceneIndexInputFormat.class);

      FileInputFormat.addInputPath(job, lucene2seqConf.getIndexPath());
      FileOutputFormat.setOutputPath(job, lucene2seqConf.getSequenceFilesOutputPath());

      job.setJarByClass(LuceneIndexToSequenceFilesJob.class);
      job.setNumReduceTasks(0);

      job.waitForCompletion(true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
