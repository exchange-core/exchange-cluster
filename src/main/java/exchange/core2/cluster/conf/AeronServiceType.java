package exchange.core2.cluster.conf;

public enum AeronServiceType {

    ARCHIVE_CONTROL_REQUEST(1),
    ARCHIVE_CONTROL_RESPONSE(2),
    CLIENT_FACING(3),
    MEMBER_FACING(4),
    LOG(5),
    TRANSFER(6),
    LOG_CONTROL(7),
    REPLICATION_CHANNEL(8);

    private final int portOffset;

    AeronServiceType(int portOffset) {
        this.portOffset = portOffset;
    }

    public int getPortOffset() {
        return portOffset;
    }

}
