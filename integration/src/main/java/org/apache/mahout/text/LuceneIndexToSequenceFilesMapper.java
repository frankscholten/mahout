package org.apache.mahout.text;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * Maps document IDs to key value pairs with ID field as the key and the concatenated field and extra fields
 * as value.
 */
public class LuceneIndexToSequenceFilesMapper extends Mapper<Text, NullWritable, Text, Text> {

  public static final String SEPARATOR_FIELDS = " ";
  public static final int USE_TERM_INFOS = 1;

  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration;
  private SegmentReader segmentReader;

  private Text idKey;
  private Text fieldValue;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);

    LuceneSegmentInputSplit inputSplit = (LuceneSegmentInputSplit) context.getInputSplit();
    SegmentInfo segmentInfo = inputSplit.getSegment(directory);
    segmentReader = SegmentReader.get(true, segmentInfo, USE_TERM_INFOS);

    idKey = new Text();
    fieldValue = new Text();
  }

  @Override
  protected void map(Text key, NullWritable text, Context context) throws IOException, InterruptedException {
    int docId = Integer.valueOf(key.toString());
    Document document = segmentReader.document(docId);

    String idString = document.get(lucene2SeqConfiguration.getIdField());

    StringBuilder valueBuilder = new StringBuilder();
    List<String> fields = lucene2SeqConfiguration.getFields();
    for (int i = 0; i < fields.size(); i++) {
      String field = fields.get(i);
      String fieldValue = document.get(field);
      if (isNotBlank(fieldValue)) {
        valueBuilder.append(fieldValue);
        if (i != fields.size() - 1) {
          valueBuilder.append(SEPARATOR_FIELDS);
        }
      }
    }

    idKey.set(nullSafe(idString));
    fieldValue.set(nullSafe(valueBuilder.toString()));

    context.write(idKey, fieldValue);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    segmentReader.close();
  }

  private String nullSafe(String value) {
    if (value == null) {
      return "";
    } else {
      return value;
    }
  }
}