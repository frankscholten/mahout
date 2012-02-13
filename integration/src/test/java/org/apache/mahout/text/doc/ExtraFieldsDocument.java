package org.apache.mahout.text.doc;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * Used for testing lucene2seq
 */
public class ExtraFieldsDocument extends SimpleDocument {

  public static final String EXTRA_FIELD1 = "extraField1";
  public static final String EXTRA_FIELD2 = "extraField2";

  private String extraField1;
  private String extraField2;

  public ExtraFieldsDocument(String id, String field, String extraField1, String extraField2) {
    super(id, field);
    this.extraField1 = extraField1;
    this.extraField2 = extraField2;
  }

  public String getExtraField1() {
    return extraField1;
  }

  public String getExtraField2() {
    return extraField2;
  }

  @Override
  public Document asLuceneDocument() {
    Document document = super.asLuceneDocument();

    Field extraField1 = new Field(EXTRA_FIELD1, this.extraField1, Field.Store.YES, Field.Index.ANALYZED);
    Field extraField2 = new Field(EXTRA_FIELD2, this.extraField2, Field.Store.YES, Field.Index.ANALYZED);

    document.add(extraField1);
    document.add(extraField2);

    return document;
  }
}
