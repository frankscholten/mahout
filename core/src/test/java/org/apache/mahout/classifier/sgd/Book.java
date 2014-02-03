package org.apache.mahout.classifier.sgd;

import org.apache.mahout.classifier.sgd.annotations.Feature;
import org.apache.mahout.classifier.sgd.annotations.Target;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.TextValueEncoder;

public class Book {

  @Target
  private String genre;

  @Feature(encoder = StaticWordValueEncoder.class)
  private String title;

  @Feature(encoder = ContinuousValueEncoder.class)
  private Integer publicationYear;

  @Feature(encoder = TextValueEncoder.class)
  protected String description;

  public Book(String title, int publicationYear, String genre, String description) {
    this.title = title;
    this.publicationYear = publicationYear;
    this.genre = genre;
    this.description = description;
  }

  public int getPublicationYear() {
    return publicationYear;
  }

  public void setPublicationYear(int publicationYear) {
    this.publicationYear = publicationYear;
  }

  public String getGenre() {
    return genre;
  }

  public void setGenre(String genre) {
    this.genre = genre;
  }

  public String getTitle() {
    return title;
  }

  public String getDescription() {
    return description;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setPublicationYear(Integer publicationYear) {
    this.publicationYear = publicationYear;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
