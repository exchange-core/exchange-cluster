package exchange.core2.cluster.conf;

import exchange.core2.cluster.ExchangeCoreCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.IntStream;

public class ClusterConfigurationsFactory {

    private static final Logger log = LoggerFactory.getLogger(ClusterConfigurationsFactory.class);

    private static final String DEFAULT_PROPERTIES_FILENAME = "exchange-cluster-default.properties";


    // TODO simplify, provide as string parameter
    public static ClusterConfiguration createClusterConfiguration(final ExchangeCoreCluster.ConfigurationType configurationType,
                                                                  final int totalNodes,
                                                                  final String propertiesFilename) {

        switch (configurationType) {
            case LOCAL:
                return new ClusterLocalConfiguration(totalNodes);

            case MULTISERVER:
                return createMultiServerConfiguration(propertiesFilename);

            default:
                throw new RuntimeException("Unsupported configuration type: " + configurationType);
        }
    }

    /**
     * @return cluster configuration
     */
    private static ClusterConfiguration createMultiServerConfiguration(final String propertiesFilename) {

        try (final InputStream is = createPropertiesStream(propertiesFilename)) {

            final Properties properties = new Properties();
            properties.load(is);

            final int nodesTotal = Integer.parseInt(properties.getProperty("nodesTotal"));
            final int basePort = Integer.parseInt(properties.getProperty("basePort"));
            final String[] nodeHosts = IntStream.range(0, nodesTotal)
                    .mapToObj(i -> properties.getProperty("nodeHost" + i))
                    .toArray(String[]::new);

            return new ClusterMultiServerConfiguration(basePort, nodeHosts);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static InputStream createPropertiesStream(final String propertiesFilename) {

        try {
            if (propertiesFilename != null) {
                log.info("Using provided configuration file: {}", propertiesFilename);
                return new FileInputStream(propertiesFilename);
            } else {
                log.info("Using default internal configuration: {}", DEFAULT_PROPERTIES_FILENAME);
                return ExchangeCoreCluster.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES_FILENAME);
            }

        } catch (final Exception ex) {
            log.error("Can not open configuration file");
            throw new RuntimeException(ex);
        }
    }


}
