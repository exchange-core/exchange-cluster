package exchange.core2.cluster.model.binary;

import exchange.core2.cluster.utils.BufferWritable;

public interface BinaryDataCommand extends BufferWritable {

    int getBinaryCommandTypeCode();

}
