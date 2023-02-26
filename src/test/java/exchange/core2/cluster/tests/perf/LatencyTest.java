package exchange.core2.cluster.tests.perf;

import exchange.core2.cluster.GenericClusterIT;
import exchange.core2.cluster.testing.LatencyTestsModule;
import exchange.core2.cluster.testing.LocalTestingContainer;
import exchange.core2.cluster.testing.TestDataParameters;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.MULTI;
import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.SINGLE_NODE;

public class LatencyTest {

    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    @Test
    public void runStressTestSingle() {

        LatencyTestsModule.latencyTestImpl(
                TestDataParameters.small(),
                handler -> LocalTestingContainer.create(handler, SINGLE_NODE),
                3);
    }

    @Test
    public void runStressTestMulti() {

        LatencyTestsModule.latencyTestImpl(
            TestDataParameters.small(),
            handler -> LocalTestingContainer.create(handler, MULTI),
            3);
    }

}
