/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.intercept;

import io.netty.buffer.ByteBuf;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String START_INDEX = "startIndex";
    private static final String INDEX = "index";
    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    private final AppendIndexMetadataInterceptor appendIndexMetadataInterceptor;
    private final Set<ManagedLedgerPayloadProcessor.Processor> inputProcessors;
    private final Set<ManagedLedgerPayloadProcessor.Processor> outputProcessors;

    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                                        Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;

        // save appendIndexMetadataInterceptor to field
        AppendIndexMetadataInterceptor appendIndexMetadataInterceptor = null;

        for (BrokerEntryMetadataInterceptor interceptor : this.brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                appendIndexMetadataInterceptor = (AppendIndexMetadataInterceptor) interceptor;
                break;
            }
        }

        this.appendIndexMetadataInterceptor = appendIndexMetadataInterceptor;

        if (brokerEntryPayloadProcessors != null) {
            this.inputProcessors = new LinkedHashSet<>();
            this.outputProcessors = new LinkedHashSet<>();
            for (ManagedLedgerPayloadProcessor processor : brokerEntryPayloadProcessors) {
                this.inputProcessors.add(processor.inputProcessor());
                this.outputProcessors.add(processor.outputProcessor());
            }
        } else {
            this.inputProcessors = null;
            this.outputProcessors = null;
        }
    }

    public long getIndex() {
        long index = -1;

        if (appendIndexMetadataInterceptor != null) {
            return appendIndexMetadataInterceptor.getIndex();
        }

        return index;
    }

    public long getStartIndex() {
        long startIndex = -1;

        if (appendIndexMetadataInterceptor != null) {
            return appendIndexMetadataInterceptor.getStartIndex();
        }

        return startIndex;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
       if (op == null || numberOfMessages <= 0) {
           return op;
       }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, numberOfMessages));
        return op;
    }

    @Override
    public CompletableFuture<Void> onTrimLedgers(String name,
                                                 ManagedLedgerImpl managedLedger,
                                                 List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgersToDelete) {
        if (appendIndexMetadataInterceptor == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> promise = new CompletableFuture<>();
        if (!ledgersToDelete.isEmpty()) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgersToDelete.get(ledgersToDelete.size() - 1);
            managedLedger.asyncReadEntry(PositionImpl.get(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            ByteBuf buf = entry.getDataBuffer();
                            try {
                                // might throw IllegalArgumentException
                                final BrokerEntryMetadata brokerEntryMetadata =
                                        Commands.peekBrokerEntryMetadataIfExist(buf);
                                if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                                    long index = brokerEntryMetadata.getIndex();
                                    appendIndexMetadataInterceptor.setStartIndex(index + 1);
                                }
                            } catch (IllegalArgumentException | IllegalStateException e) {
                                // This exception could be thrown by both peekBrokerEntryMetadataIfExist or null check
                                log.info("[{}] Failed to read brokerEntryMetadata while trimming", name, e);
                            } finally {
                                entry.release();
                                promise.complete(null);
                            }
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            log.warn("[{}] Failed to read last entry of ledger {} while trimming", name,
                                    ledgerInfo.getLedgerId(), exception);
                            promise.complete(null);
                        }
                    }, null);
        } else {
            promise.complete(null);
        }
        return promise;
    }

    @Override
    public void afterFailedAddEntry(int numberOfMessages) {
        if (appendIndexMetadataInterceptor != null) {
            appendIndexMetadataInterceptor.decreaseWithNumberOfMessages(numberOfMessages);
        }
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(INDEX)) {
            if (appendIndexMetadataInterceptor != null) {
                appendIndexMetadataInterceptor.recoveryIndexGenerator(
                        Long.parseLong(propertiesMap.get(INDEX)));
            }
        }
        if (propertiesMap.containsKey(START_INDEX)) {
            if (appendIndexMetadataInterceptor != null) {
                appendIndexMetadataInterceptor.setStartIndex(
                        Long.parseLong(propertiesMap.get(START_INDEX)));
            }
        }
    }

    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        if (this.appendIndexMetadataInterceptor != null && lh.getLastAddConfirmed() >= 0) {
            lh.readAsync(lh.getLastAddConfirmed(), lh.getLastAddConfirmed()).whenComplete((entries, ex) -> {
                if (ex != null) {
                    log.error("[{}] Read last entry error.", name, ex);
                    promise.completeExceptionally(ex);
                } else {
                    if (entries != null) {
                        try {
                            LedgerEntry ledgerEntry = entries.getEntry(lh.getLastAddConfirmed());
                            if (ledgerEntry != null) {
                                BrokerEntryMetadata brokerEntryMetadata =
                                        Commands.parseBrokerEntryMetadataIfExist(ledgerEntry.getEntryBuffer());
                                if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                                    appendIndexMetadataInterceptor.recoveryIndexGenerator(
                                            brokerEntryMetadata.getIndex());
                                }
                            }
                            entries.close();
                            promise.complete(null);
                        } catch (Exception e) {
                            entries.close();
                            log.error("[{}] Failed to recover the index generator from the last add confirmed entry.",
                                    name, e);
                            promise.completeExceptionally(e);
                        }
                    } else {
                        promise.complete(null);
                    }
                }
            });
        } else {
            promise.complete(null);
        }
        return promise;
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        if (appendIndexMetadataInterceptor != null) {
            propertiesMap.put(START_INDEX, String.valueOf(appendIndexMetadataInterceptor.getStartIndex()));
            propertiesMap.put(INDEX, String.valueOf(appendIndexMetadataInterceptor.getIndex()));
        }
    }

    private PayloadProcessorHandle processPayload(Set<ManagedLedgerPayloadProcessor.Processor> processors,
                                                  Object context, ByteBuf payload) {

        ByteBuf tmpData = payload;
        final Set<ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf>> processedSet = new LinkedHashSet<>();
        for (ManagedLedgerPayloadProcessor.Processor payloadProcessor : processors) {
            if (payloadProcessor != null) {
                tmpData = payloadProcessor.process(context, tmpData);
                processedSet.add(new ImmutablePair<>(payloadProcessor, tmpData));
            }
        }
        final ByteBuf dataToReturn = tmpData;
        return new PayloadProcessorHandle() {
            @Override
            public ByteBuf getProcessedPayload() {
                return dataToReturn;
            }

            @Override
            public void release() {
                for (ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf> p : processedSet) {
                    p.left.release(p.right);
                }
                processedSet.clear();
            }
        };
    }
    @Override
    public PayloadProcessorHandle processPayloadBeforeLedgerWrite(OpAddEntry op, ByteBuf ledgerData) {
        if (this.inputProcessors == null || this.inputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.inputProcessors, op.getCtx(), ledgerData);
    }

    @Override
    public PayloadProcessorHandle processPayloadBeforeEntryCache(ByteBuf ledgerData){
        if (this.outputProcessors == null || this.outputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.outputProcessors, null, ledgerData);
    }
}
