Streaming K-means Reuters Example

Work in progress!

First build and add streaming k-means to your maven repo

    $ git clone https://github.com/tdunning/knn
    $ cd knn
    $ mvn clean install

Now build this tree

    $ mvn clean install -DskipTests=true
    $ cd  examples/bin

And run the reuters demo and select stream, option 2

    $ cluster-reuters.sh

TODO: Make StreamingKMeansDriver support seq file with vectors instead of a Iterable<MatrixSlice> object

---

Welcome to Apache Mahout!

Mahout is a scalable machine learning library that implements many different
approaches to machine learning.  The project currently contains
implementations of algorithms for classification, clustering, frequent item
set mining, genetic programming and collaborative filtering. Mahout is
scalable along three dimensions: It scales to reasonably large data sets by
leveraging algorithm properties or implementing versions based on Apache
Hadoop. It scales to your perferred business case as it is distributed under
a commercially friendly license. In addition it scales in terms of support
by providing a vibrant, responsive and diverse community.

Getting Started

 See https://cwiki.apache.org/MAHOUT/quickstart.html

To compile the sources run 'mvn clean install'
To run all the tests run 'mvn test'
To setup your ide run 'mvn eclipse:eclipse' or 'mvn idea:idea'
For more info on maven see http://maven.apache.org

For more information on how to contribute see:

 https://cwiki.apache.org/confluence/display/MAHOUT/How+To+Contribute


Legal

 Please see the NOTICE.txt included in this directory for more information.

Documentation

See http://mahout.apache.org/.
