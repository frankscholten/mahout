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

package org.apache.mahout.classifier.naivebayes;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import com.google.common.base.Preconditions;

/** NaiveBayesModel holds the weight Matrix, the feature and label sums and the weight normalizer vectors.*/
public class NaiveBayesModel {

  final Vector weightsPerLabel;
  final Vector perlabelThetaNormalizer;
  //  private final double minThetaNormalizer;
  final Vector weightsPerFeature;
  final Matrix weightsPerLabelAndFeature;
  private final float alphaI;
  private final double numFeatures;
  private final double totalWeightSum;

  public NaiveBayesModel(Matrix weightMatrix,
                         Vector weightsPerFeature,
                         Vector weightsPerLabel,
                         Vector thetaNormalizer,
                         float alphaI) {
    this.weightsPerLabelAndFeature = weightMatrix;
    this.weightsPerFeature = weightsPerFeature;
    this.weightsPerLabel = weightsPerLabel;
    this.perlabelThetaNormalizer = thetaNormalizer;
    this.numFeatures = weightsPerFeature.getNumNondefaultElements();
    this.totalWeightSum = weightsPerLabel.zSum();
    this.alphaI = alphaI;
//    this.minThetaNormalizer = thetaNormalizer.maxValue();
  }

  public double labelWeight(int label) {
    return weightsPerLabel.getQuick(label);
  }

//  public double thetaNormalizer(int label) {
//    return perlabelThetaNormalizer.get(label) / minThetaNormalizer;
//  }

  public double featureWeight(int feature) {
    return weightsPerFeature.getQuick(feature);
  }

  public double weight(int label, int feature) {
    return weightsPerLabelAndFeature.getQuick(label, feature);
  }

  public float alphaI() {
    return alphaI;
  }

  public double numFeatures() {
    return numFeatures;
  }

  public double totalWeightSum() {
    return totalWeightSum;
  }
  
  public int numLabels() {
    return weightsPerLabel.size();
  }

  public Vector createScoringVector() {
    return weightsPerLabel.like();
  }

  public void validate() {
    Preconditions.checkState(alphaI > 0, "alphaI has to be greater than 0!");
    Preconditions.checkArgument(numFeatures > 0, "the vocab count has to be greater than 0!");
    Preconditions.checkArgument(totalWeightSum > 0, "the totalWeightSum has to be greater than 0!");
    Preconditions.checkNotNull(weightsPerLabel, "the number of labels has to be defined!");
    Preconditions.checkArgument(weightsPerLabel.getNumNondefaultElements() > 0,
        "the number of labels has to be greater than 0!");
    Preconditions.checkArgument(perlabelThetaNormalizer != null, "the theta normalizers have to be defined");
    // Preconditions.checkArgument(perlabelThetaNormalizer.getNumNondefaultElements() > 0,
    //    "the number of theta normalizers has to be greater than 0!");
    Preconditions.checkNotNull(weightsPerFeature, "the feature sums have to be defined");
    Preconditions.checkArgument(weightsPerFeature.getNumNondefaultElements() > 0,
        "the feature sums have to be greater than 0!");
    // Check if all thetas have same sign.
    /*Iterator<Element> it = perlabelThetaNormalizer.iterateNonZero();
    while (it.hasNext()) {
      Element e = it.next();
      Preconditions.checkArgument(Math.signum(e.get()) == Math.signum(minThetaNormalizer), e.get()
          + "  " + minThetaNormalizer);
    }*/
  }
}
