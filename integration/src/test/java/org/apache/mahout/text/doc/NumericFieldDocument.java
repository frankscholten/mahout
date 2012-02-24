package org.apache.mahout.text.doc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;

/**
 * Document with numeric field.
 */
public class NumericFieldDocument extends SingleFieldDocument {

  public static final String NUMERIC_FIELD = "numeric";

  private int numericField;

  public NumericFieldDocument(String id, String field, int numericField) {
    super(id, field);
    this.numericField = numericField;
  }

  @Override
  public Document asLuceneDocument() {
    Document document = new Document();

    document.add(new Field(ID_FIELD, getId(), Field.Store.YES, Field.Index.NO));
    document.add(new Field(FIELD, getField(), Field.Store.YES, Field.Index.ANALYZED));
    document.add(new NumericField(NUMERIC_FIELD, Field.Store.YES, false).setIntValue(numericField));

    return document;
  }

  public int getNumericField() {
    return numericField;
  }
}
