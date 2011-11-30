package org.apache.mahout.clustering.dirichlet;

import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.Model;
import org.apache.mahout.clustering.ModelDistribution;
import org.apache.mahout.clustering.dirichlet.models.DistanceMeasureClusterDistribution;
import org.apache.mahout.clustering.dirichlet.models.DistributionDescription;
import org.apache.mahout.clustering.dirichlet.models.GaussianCluster;
import org.apache.mahout.clustering.dirichlet.models.GaussianClusterDistribution;
import org.apache.mahout.common.MahoutTestCase;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

public class DirichletStateTest extends MahoutTestCase {

    private DirichletState state;
    private int numClusters;
    private ModelDistribution<VectorWritable> modelDistribution;
    private double alpha0;
    private DenseVector vector;
    private List<DirichletCluster> clusters;
    private VectorWritable prototype;

    private GaussianCluster model1;
    private GaussianCluster model2;
    private GaussianCluster model3;

    @Before
    public void before() {
        vector = new DenseVector(new double[]{0.1, 0.5});
        prototype = new VectorWritable(vector);
        modelDistribution = new GaussianClusterDistribution(prototype);
        alpha0 = 0.001;

        model1 = new GaussianCluster(new DenseVector(new double[]{0.1, 0.2}), 0);
        model2 = new GaussianCluster(new DenseVector(new double[]{0.3, 0.4}), 1);
        model3 = new GaussianCluster(new DenseVector(new double[]{0.4, 0.5}), 2);

        DirichletCluster cluster1 = new DirichletCluster(model1, 1);
        DirichletCluster cluster2 = new DirichletCluster(model2, 2);
        DirichletCluster cluster3 = new DirichletCluster(model3, 3);
        clusters = asList(cluster1, cluster2, cluster3);
        numClusters = clusters.size();

        state = new DirichletState(modelDistribution, numClusters, alpha0);
        state.setClusters(clusters);
    }

    @Test
    public void testConstructor_distributionDescription() {
        String modelDistributionClass = modelDistribution.getClass().getName();
        String prototypeClass = vector.getClass().getName();
        String distanceMeasureClass = EuclideanDistanceMeasure.class.getName();
        DistributionDescription description = new DistributionDescription(modelDistributionClass, prototypeClass, distanceMeasureClass, vector.size());
        state = new DirichletState(description, numClusters, alpha0);

        assertThat(state.getModelFactory(), instanceOf(ModelDistribution.class));
        assertThat(state.getMixture().size(), is(equalTo(numClusters)));
        assertThat(state.getMixture().zSum(), is(closeTo(1.0, EPSILON)));
        assertThat(state.getNumClusters(), is(sameInstance(numClusters)));
    }

    @Test
    public void testUpdateModels() {
        DirichletState state = new DirichletState(modelDistribution, 1, alpha0);
        GaussianCluster model = new GaussianCluster(new DenseVector(new double[]{0.1, 0.2}), 0);
        state.setClusters(asList(new DirichletCluster(model)));

        assertThat(state.getModels()[0].count(), is(equalTo(0L)));

        model.observe(new DenseVector(new double[]{1, 0}));
        model.observe(new DenseVector(new double[]{0, 1}));

        state.update(new Cluster[]{model});

        assertThat(state.getModels()[0].count(), is(equalTo(2L)));
    }

    @Test
    public void testGetModelFactory() {
        assertThat(state.getModelFactory(), is(sameInstance(modelDistribution)));
    }

    @Test
    public void testGetMixture() {
        Vector mixture = state.getMixture();

        assertThat(mixture.size(), is(equalTo(numClusters)));
        assertThat(mixture.zSum(), is(closeTo(1.0, EPSILON)));
    }

    @Test
    public void testSetClusters() {
        state.setClusters(clusters);

        assertThat(state.getClusters(), is(sameInstance(clusters)));
    }

    @Test
    public void testTotalCount() {
        // TODO: Replace dense vectors by random vectors

        DirichletCluster cluster1 = new DirichletCluster(model1, 1);
        DirichletCluster cluster2 = new DirichletCluster(model2, 2);
        DirichletCluster cluster3 = new DirichletCluster(model3, 3);
        List<DirichletCluster> clusters = asList(cluster1, cluster2, cluster3);

        state = new DirichletState(modelDistribution, clusters.size(), alpha0);
        state.setClusters(clusters);

        assertThat(state.totalCounts(), is(equalTo((Vector) new DenseVector(new double[]{1, 2, 3}))));
    }

    @Test
    public void testGetModels() {
        assertArrayEquals(state.getModels(), new Model[]{model1, model2, model3});
    }

    @Test
    public void testAdjustedProbability() {
        DenseVector vector = new DenseVector(new double[]{0.1, 0.2});
        VectorWritable vectorWritable = new VectorWritable(vector);

        int index = 0;
        double pdf = 0.5;

        Cluster cluster = createMock(Cluster.class);
        expect(cluster.pdf(vectorWritable)).andReturn(pdf);
        replay(cluster);

        DirichletCluster cluster1 = new DirichletCluster(cluster, 1);
        List<DirichletCluster> clusters = asList(cluster1);

        state = new DirichletState(modelDistribution, clusters.size(), alpha0);
        state.setClusters(clusters);

        double mix = state.getMixture().get(index);

        assertThat(state.adjustedProbability(vectorWritable, index), is(equalTo(pdf * mix)));
    }

    @Test
    public void testSetModelFactory() {
        ModelDistribution<VectorWritable> modelFactory = new DistanceMeasureClusterDistribution(prototype, new EuclideanDistanceMeasure());
        state.setModelFactory(modelFactory);

        assertThat(state.getModelFactory(), is(sameInstance(modelFactory)));
    }

    @Test
    public void testSetNumClusters() {
        int numClusters = 20;

        state.setNumClusters(numClusters);

        assertThat(state.getNumClusters(), is(equalTo(numClusters)));
    }

    @Test
    public void testSetMixture() {
        Vector mixture = new DenseVector(new double[] {0.1, 0.4});
        state.setMixture(mixture);

        assertThat(state.getMixture(), is(equalTo(mixture)));
    }
}
