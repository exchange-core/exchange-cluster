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

import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.utils.SerializationUtils;
import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collection;

public final class BatchAddSymbolsCommand implements BinaryDataCommand<BatchAddSymbolsResult> {

    private final IntObjectHashMap<CoreSymbolSpecification> symbols;

    public BatchAddSymbolsCommand(final CoreSymbolSpecification symbol) {
        symbols = IntObjectHashMap.newWithKeysValues(symbol.getSymbolId(), symbol);
    }

    public BatchAddSymbolsCommand(final Collection<CoreSymbolSpecification> collection) {
        symbols = new IntObjectHashMap<>(collection.size());
        collection.forEach(s -> symbols.put(s.getSymbolId(), s));
    }

    public BatchAddSymbolsCommand(final BufferReader bytes) {
        symbols = SerializationUtils.readIntHashMap(bytes, CoreSymbolSpecification::new);
    }

    public IntObjectHashMap<CoreSymbolSpecification> getSymbols() {
        return symbols;
    }

    @Override
    public void writeToBuffer(BufferWriter bytes) {
        SerializationUtils.marshallIntHashMap(symbols, bytes);
    }

    @Override
    public short getBinaryCommandTypeCode() {
        return BinaryCommandType.ADD_SYMBOLS.getCode();
    }

}
