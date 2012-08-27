package org.apache.mahout.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.search.BooleanQuery;
import org.junit.Test;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LuceneStorageConfigurationTest {
  
  @Test
  public void testSerialization() throws Exception {
    Configuration conf = new Configuration();
    Path indexPath = new Path("indexPath");
    Path outputPath = new Path("outputPath");
    LuceneStorageConfiguration luceneStorageConf = new LuceneStorageConfiguration(conf, asList(indexPath), outputPath, "id", asList("field"));

    Configuration serializedConf = luceneStorageConf.serialize();

    LuceneStorageConfiguration deserializedConf = new LuceneStorageConfiguration(serializedConf);

    assertEquals(luceneStorageConf, deserializedConf);
    assertEquals(Integer.MAX_VALUE, BooleanQuery.getMaxClauseCount());
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSerialization_notSerialized() throws IOException {
    new LuceneStorageConfiguration(new Configuration());
  }
}
