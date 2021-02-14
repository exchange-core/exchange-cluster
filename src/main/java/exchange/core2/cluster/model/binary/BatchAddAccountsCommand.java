/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.cluster.model.binary;

import exchange.core2.cluster.utils.SerializationUtils;
import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;


public final class BatchAddAccountsCommand implements BinaryDataCommand {

    private final LongObjectHashMap<IntLongHashMap> users;

    public BatchAddAccountsCommand(final BufferReader bytes) {
        users = SerializationUtils.readLongHashMap(bytes, c -> SerializationUtils.readIntLongHashMap(bytes));
    }

    @Override
    public void writeToBuffer(final BufferWriter bytes) {
        SerializationUtils.marshallLongHashMap(users, SerializationUtils::marshallIntLongHashMap, bytes);
    }

    @Override
    public short getBinaryCommandTypeCode() {
        return BinaryCommandType.ADD_ACCOUNTS.getCode();
    }
}
