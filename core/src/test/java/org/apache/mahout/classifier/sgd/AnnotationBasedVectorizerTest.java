package org.apache.mahout.classifier.sgd;

import org.apache.mahout.classifier.sgd.annotations.Feature;
import org.apache.mahout.classifier.sgd.annotations.Target;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class AnnotationBasedVectorizerTest {

  private AnnotationBasedVectorizer<Book> vectorizer = new AnnotationBasedVectorizer<Book>(new TypeReference<Book>() {});

  private Book starking;
  private Book hobbit;

  @Before
  public void before() {
    starking = new Book("Star King", 1963, "Sci-fi", "Kirth Gersen carries in his pocket a slip of paper with a list of five names" +
        "written on it. Theses are the names of the five Demon Princes who led the historic Mount Pleasant Massacre, " +
        "which destroyed not only Kirth's family but his entrire world as well. He roams the universe, searching the " +
        "endless galaxies of space, hunting down the Demon Princes and exacting his revenge. Three princes will fall " +
        "before Kirth's work is done, and two more await their doom...");
    hobbit = new Book("The Hobbit", 1937, "Fantasy", "If you care for journeys there and back, out of the comfortable" +
        "Western world, over the edge of the Wild, and home again, and can take an interest in a humble hero" +
        "(blessed with a little wisdom and a little courage and considerable good luck), here is a record of such a" +
        "journey and such a traveler. The period is the ancient time between the age of Faerie and the dominion of men," +
        "when the famous forest of Mirkwood was still standing, and the mountains were full of danger. In following the" +
        "path of this humble adventurer, you will learn by the way (as he did) -- if you do not already know all about" +
        "these things -- much about trolls, goblins, dwarves, and elves, and get some glimpses into the history and" +
        "politics of a neglected but important period. For Mr. Bilbo Baggins visited various notable persons; conversed" +
        "with the dragon, Smaug the Magnificent; and was present, rather unwillingly, at the Battle of the Five Armies." +
        "This is all the more remarkable, since he was a hobbit. Hobbits have hitherto been passed over in history and" +
        "legend, perhaps because they as a rule preferred comfort to excitement. But this account, based on his" +
        "personal memoirs, of the one exciting year in the otherwise quiet life of Mr. Baggins will give you a fair" +
        "idea of the estimable people now (it is said) becoming rather rare. They do not like noise.");
  }

  @Test
  public void testNumberOfFeatures() {
    assertEquals(4, vectorizer.getNumberOfFeatures());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullObject() {
    vectorizer.vectorize(null);
  }

  @Test(expected = RuntimeException.class)
  public void testNoFeatureAnnotations() {
    class Dummy { }
    new AnnotationBasedVectorizer<Dummy>(new TypeReference<Dummy>() {});
  }

  @Test
  public void testVectorize() {
    Vector starKingVector = vectorizer.vectorize(starking);
    Vector hobbitVector = vectorizer.vectorize(starking);
    System.out.println(starKingVector.getNumNonZeroElements());
    System.out.println(hobbitVector.getNumNonZeroElements());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTarget_nullObject() {
    vectorizer.getTarget(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTarget_noTargetAnnotation() {
    class Dummy { }
    new AnnotationBasedVectorizer<Dummy>(new TypeReference<Dummy>() {});
  }

  @Test(expected = IncompatibleEncoderException.class)
  public void testVectorize_continuousValueEncoderOnString() {
    class Dummy {

      @Feature(encoder = ContinuousValueEncoder.class)
      String word;

      void setWord(String word) {
        this.word = word;
      }

      @Target
      int target;
    }

    Dummy dummy = new Dummy();
    dummy.setWord("word");

    AnnotationBasedVectorizer<Dummy> vectorizer = new AnnotationBasedVectorizer<Dummy>(new TypeReference<Dummy>(){});
    vectorizer.vectorize(dummy);
  }

  @Test
  public void testGetTarget() {
    assertEquals(0, vectorizer.getTarget(starking));
    assertEquals(1, vectorizer.getTarget(hobbit));
  }
}
