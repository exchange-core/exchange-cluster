package exchange.core2.cluster.utils;

import exchange.core2.cluster.conf.AeronServiceType;
import exchange.core2.cluster.conf.ClusterConfiguration;


public class NetworkUtils {

    public static String ingressEndpoints(final ClusterConfiguration conf) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < conf.getTotalNodes(); i++) {
            sb.append(i).append('=');
            sb.append(conf.getNodeEndpoint(i, AeronServiceType.CLIENT_FACING));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        return sb.toString();
    }


    public static String clusterMembers(final ClusterConfiguration conf) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < conf.getTotalNodes(); i++) {
            sb.append(i);
            sb.append(',').append(conf.getNodeEndpoint(i, AeronServiceType.CLIENT_FACING));
            sb.append(',').append(conf.getNodeEndpoint(i, AeronServiceType.MEMBER_FACING));
            sb.append(',').append(conf.getNodeEndpoint(i, AeronServiceType.LOG));
            sb.append(',').append(conf.getNodeEndpoint(i, AeronServiceType.TRANSFER));
            sb.append(',').append(conf.getNodeEndpoint(i, AeronServiceType.ARCHIVE_CONTROL_REQUEST));
            sb.append('|');
        }

        return sb.toString();
    }
}
