package org.apache.mahout.classifier.sgd;

import org.apache.mahout.classifier.sgd.annotations.Feature;
import org.apache.mahout.classifier.sgd.annotations.Target;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jackson.type.TypeReference;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Vectorizes objects of type T annotated with @Feature and @Target annotations.
 *
 * Requires a 'super type token' anonymous subclass on construction to inspect @Feature and @Target annotations of the
 * type T at runtime.
 *
 * @see <a href="http://gafter.blogspot.nl/2006/12/super-type-tokens.html">Neal Gafter's blog on super type tokens.</a>
 */
public class AnnotationBasedVectorizer<T> {

  public int vectorSize = 1000;

  private Dictionary dictionary = new Dictionary();

  private Map<String, FeatureVectorEncoder> encoderMap = new HashMap<String, FeatureVectorEncoder>();

  public AnnotationBasedVectorizer() {

  }

  public AnnotationBasedVectorizer(int vectorSize) {
    this.vectorSize = vectorSize;
  }

  @SuppressWarnings("unchecked")
  public AnnotationBasedVectorizer(TypeReference<T> typeReference) {
    TypeFactory typeFactory = TypeFactory.defaultInstance();
    JavaType javaType = typeFactory.constructType(typeReference);
    Field[] fields = javaType.getRawClass().getDeclaredFields();

    int targetAnnotations = 0;
    for (Field field : fields) {
      Feature feature = field.getAnnotation(Feature.class);
      if (feature != null) {
        field.setAccessible(true);
        encoderMap.put(field.getName(), createFeatureEncoder(field.getName(), feature));
      } else if (field.getAnnotation(Target.class) != null) {
        targetAnnotations++;
      }
    }

    if (encoderMap.size() == 0) {
      throw new IllegalArgumentException("No @Feature annotations found on type " + javaType.getRawClass().getName());
    }
    if (targetAnnotations == 0) {
      throw new IllegalArgumentException("No @Target annotation found on type " + javaType.getRawClass().getName());
    } else if (targetAnnotations > 1) {
      throw new IllegalArgumentException("Multiple @Target annotations found on type " + javaType.getRawClass().getName());
    }
  }


  /**
   * Vectorizes the given object.
   *
   * @param object to be vectorized
   *
   * @return vector
   */
  public Vector vectorize(T object) {
    if (object == null) {
      throw new IllegalArgumentException("Object cannot be null.");
    }

    RandomAccessSparseVector vector = new RandomAccessSparseVector(vectorSize);
    for (Map.Entry<String, FeatureVectorEncoder> fieldEncoder : encoderMap.entrySet()) {
      encodeFieldInVector(vector, object, fieldEncoder);
    }
    return vector;
  }

  /**
   * Returns the integer coding of the target class of the given object. The object
   * should have a @Target annotation on a field with the target value.
   * Uses a {@link Dictionary} to translate between target values and their integer encodings, when they appear.
   *
   * @param object that has a field annotated with @Class
   *
   * @return integer code of @Target value
   */
  public int getTarget(T object) {
    if (object == null) {
      throw new IllegalArgumentException("Object cannot be null.");
    }

    try {
      String target = doGetTarget(object);
      return dictionary.intern(target);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not determine target of object", e);
    }
  }

  public int getNumberOfFeatures() {
    return encoderMap.size();
  }

  //========================================== Helpers ===============================================================//

  private void checkEncoderApplicableForField(Field field, FeatureVectorEncoder encoder) {
    if (encoder instanceof ContinuousValueEncoder) {
      if (!(field.getType().equals(Integer.class) || (field.getType().equals(Double.class)) ||
          (field.getType().equals(Float.class)))) {
        throw new IncompatibleEncoderException(encoder.getClass() + "' does not support type '" + field.getType() + "' of field '" + field.getName() + "'");
      }
    }
  }

  private void encodeFieldInVector(Vector vector, T object, Map.Entry<String, FeatureVectorEncoder> fieldEncoder)  {
    try {
      Field field = object.getClass().getDeclaredField(fieldEncoder.getKey());
      field.setAccessible(true);

      Object fieldValue = field.get(object);
      if (fieldValue == null) {
        throw new IllegalArgumentException("Field '" + field.getName() + "' has a null value");
      }

      checkEncoderApplicableForField(field, fieldEncoder.getValue());

      fieldEncoder.getValue().addToVector(String.valueOf(fieldValue), vector);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Could not find field.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not access field.", e);
    }
  }

  private String doGetTarget(T object) throws IllegalAccessException {
    Field[] fields = object.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (field.getAnnotation(Target.class) != null) {
        field.setAccessible(true);
        return String.valueOf(field.get(object));
      }
    }
    // Should not happen because already checked during construction
    throw new IllegalArgumentException("Object does not have @Target field.");
  }

  private FeatureVectorEncoder createFeatureEncoder(String name, Feature feature) {
    try {
      FeatureVectorEncoder featureVectorEncoder = feature.encoder().getConstructor(String.class).newInstance(name);
      featureVectorEncoder.setProbes(feature.probes());
      return  featureVectorEncoder;
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Cannot instantiate feature vector encoder class: " + feature.encoder(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Cannot instantiate feature vector encoder class: " + feature.encoder(), e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Cannot instantiate feature vector encoder class: " + feature.encoder(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Cannot instantiate feature vector encoder class: " + feature.encoder(), e);
    }
  }

}
