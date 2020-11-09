package exchange.core2.cluster.model.binary;

import exchange.core2.cluster.utils.BufferWritable;

public interface BinaryDataResult extends BufferWritable {

    int getResultCode();

}
