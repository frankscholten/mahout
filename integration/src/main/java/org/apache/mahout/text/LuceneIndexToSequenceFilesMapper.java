package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * Maps document IDs to key value pairs with ID field as the key and the concatenated field and extra fields
 * as value.
 */
public class LuceneIndexToSequenceFilesMapper extends Mapper<NullWritable, NullWritable, Text, Text> {

  public static final String SEPARATOR_EXTRA_FIELDS = " ";

  private LuceneIndexToSequenceFilesConfiguration lucene2SeqConfiguration;
  private IndexReader indexReader;
  private IndexSearcher indexSearcher;
  private Scorer scorer;
  private Text idKey;
  private Text fieldValue;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    lucene2SeqConfiguration = new LuceneIndexToSequenceFilesConfiguration().getFromConfiguration(configuration);

    FileSystemDirectory directory = new FileSystemDirectory(FileSystem.get(configuration), lucene2SeqConfiguration.getIndexPath(), false, configuration);
    indexReader = IndexReader.open(directory);
    indexSearcher = new IndexSearcher(indexReader);

    Weight weight = lucene2SeqConfiguration.getQuery().createWeight(indexSearcher);
    scorer = weight.scorer(indexReader, true, false);

    LuceneInputSplit inputSplit = (LuceneInputSplit) context.getInputSplit();
    int docsSkipped = 0;
    while (docsSkipped < inputSplit.getPos()) {
      scorer.nextDoc();
      docsSkipped++;
    }
    
    idKey = new Text();
    fieldValue = new Text();
  }

  @Override
  protected void map(NullWritable key, NullWritable text, Context context) throws IOException, InterruptedException {
    int docId = scorer.nextDoc();
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
    indexSearcher.close();
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