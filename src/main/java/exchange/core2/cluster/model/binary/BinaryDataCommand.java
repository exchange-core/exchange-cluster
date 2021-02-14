package exchange.core2.cluster.model.binary;

import exchange.core2.cluster.utils.BufferWritable;

public interface BinaryDataCommand<T extends BinaryDataResult> extends BufferWritable {

    short getBinaryCommandTypeCode();

}
