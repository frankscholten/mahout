package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * Maps document IDs to key value pairs with ID field as the key and the concatenated field and extra fields
 * as value.
 */
public class LuceneIndexToSequenceFilesMapper extends Mapper<Text, NullWritable, Text, Text> {

  public static final String SEPARATOR_EXTRA_FIELDS = " ";

  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration;
  private IndexReader indexReader;

  private Text idKey;
  private Text fieldValue;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);
    indexReader = IndexReader.open(directory);

    idKey = new Text();
    fieldValue = new Text();
  }

  @Override
  protected void map(Text key, NullWritable text, Context context) throws IOException, InterruptedException {
    int docId = Integer.valueOf(key.toString());
    Document document = indexReader.document(docId);

    String idString = document.get(lucene2SeqConfiguration.getIdField());
    String field = document.get(lucene2SeqConfiguration.getField());

    StringBuilder valueBuilder = new StringBuilder(nullSafe(field));
    List<String> extraFields = lucene2SeqConfiguration.getExtraFields();
    for (String extraField : extraFields) {
      if (isNotBlank(document.get(extraField))) {
        valueBuilder.append(SEPARATOR_EXTRA_FIELDS).append(document.get(extraField));
      }
    }

    idKey.set(nullSafe(idString));
    fieldValue.set(nullSafe(valueBuilder.toString()));

    context.write(idKey, fieldValue);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    indexReader.close();
  }

  private String nullSafe(String value) {
    if (value == null) {
      return "";
    } else {
      return value;
    }
  }
}