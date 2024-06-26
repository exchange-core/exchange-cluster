package exchange.core2.cluster.tests.perf;

import exchange.core2.cluster.testing.LocalTestingContainer;
import exchange.core2.cluster.testing.TestDataParameters;
import exchange.core2.cluster.testing.ThroughputTestsModule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.MULTI;
import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.SINGLE_NODE;

public class ThroughputTest {

    private static final Logger log = LoggerFactory.getLogger(ThroughputTest.class);

    @Test
    public void runStressTestSingle() {

        ThroughputTestsModule.throughputTestImpl(
                TestDataParameters.small(),
            handler -> LocalTestingContainer.create(handler, SINGLE_NODE),
                10);
    }
    @Test
    public void runStressTestMulti() {

        ThroughputTestsModule.throughputTestImpl(
            TestDataParameters.small(),
            handler -> LocalTestingContainer.create(handler, MULTI),
            10);
    }
}
