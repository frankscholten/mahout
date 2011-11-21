package org.apache.mahout.clustering.dirichlet;

import org.apache.mahout.clustering.ModelDistribution;
import org.apache.mahout.clustering.dirichlet.models.GaussianClusterDistribution;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class DirichletStateTest extends MahoutTestCase {

    private DirichletState state;
    private int numClusters;

    @Before
    public void before() {
        DenseVector prototype = new DenseVector(new double[]{0.1, 0.5});
        ModelDistribution<VectorWritable> modelDistribution = new GaussianClusterDistribution(new VectorWritable(prototype));
        numClusters = 10;
        double alpha0 = 0.2;

        state = new DirichletState(modelDistribution, numClusters, alpha0);
    }

    @Test
    public void testGetMixture() {
        Vector mixture = state.getMixture();

        assertThat(mixture.size(), is(equalTo(numClusters)));
        assertThat(mixture.zSum(), is(closeTo(1.0, EPSILON)));
    }

    @Test
    public void testGetClusters() {
        assertThat(state.getClusters().size(), is(equalTo(numClusters)));
    }

}
