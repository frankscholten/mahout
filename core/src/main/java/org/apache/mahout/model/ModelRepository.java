package org.apache.mahout.model;

/**
 * Repository for retrieving and storing Mahout models.
 */
public interface ModelRepository<T> {

  T readModel();

  void writeModel(T model);

}
