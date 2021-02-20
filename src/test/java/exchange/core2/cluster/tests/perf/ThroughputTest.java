package exchange.core2.cluster.tests.perf;

import exchange.core2.cluster.GenericClusterIT;
import exchange.core2.cluster.testing.SingleNodeTestingContainer;
import exchange.core2.cluster.testing.TestDataParameters;
import exchange.core2.cluster.testing.ThroughputTestsModule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputTest {

    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    @Test
    public void runStressTest() {

        ThroughputTestsModule.throughputTestImpl(
                TestDataParameters.small(),
                SingleNodeTestingContainer::create,
                10);
    }

}
