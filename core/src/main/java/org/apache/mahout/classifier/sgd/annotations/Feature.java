package org.apache.mahout.classifier.sgd.annotations;

import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Feature {

  public Class<? extends FeatureVectorEncoder> encoder() default FeatureVectorEncoder.NoneEncoder.class;

  public int probes() default 2;
}
