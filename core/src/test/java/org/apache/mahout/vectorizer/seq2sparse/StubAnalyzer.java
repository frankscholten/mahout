package org.apache.mahout.vectorizer.seq2sparse;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Collections;

public final class StubAnalyzer extends Analyzer {
  private final Analyzer a;

  public StubAnalyzer() {
    a = new StandardAnalyzer(Version.LUCENE_31, Collections.emptySet());
  }

  @Override
  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return a.tokenStream(fieldName, reader);
  }
}
