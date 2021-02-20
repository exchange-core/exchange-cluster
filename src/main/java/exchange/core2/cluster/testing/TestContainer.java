package exchange.core2.cluster.testing;

public interface TestContainer extends AutoCloseable {

    TestingHelperClient getTestingHelperClient();

    void close();

}
