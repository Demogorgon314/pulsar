/**
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
package org.apache.pulsar.compaction;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.DelayedAckReaderImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compaction will go through the topic in two passes. The first pass
 * selects latest offset for each key in the topic. Then the second pass
 * writes these values to a ledger.
 *
 * <p>The two passes are required to avoid holding the payloads of each of
 * the latest values in memory, as the payload can be many orders of
 * magnitude larger than a message id.
 */
public class StrategicTwoPhaseCompactor extends TwoPhaseCompactor {
    private static final Logger log = LoggerFactory.getLogger(StrategicTwoPhaseCompactor.class);
    private static final int MAX_OUTSTANDING = 500;
    private final Duration phaseOneLoopReadTimeout;

    public StrategicTwoPhaseCompactor(ServiceConfiguration conf,
                                      PulsarClient pulsar,
                                      BookKeeper bk,
                                      ScheduledExecutorService scheduler) {
        super(conf, pulsar, bk, scheduler);
        phaseOneLoopReadTimeout = Duration.ofSeconds(conf.getBrokerServiceCompactionPhaseOneLoopTimeInSeconds());


    }

    public CompletableFuture<Long> compact(String topic) {
        throw new UnsupportedOperationException();
    }


    public <T> CompletableFuture<Long> compact(String topic,
                                               TopicCompactionStrategy<T> strategy) {

        return pulsar.newReader(strategy.getSchema())
                .topic(topic)
                .subscriptionName(COMPACTION_SUBSCRIPTION)
                .startMessageId(MessageId.earliest)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(30, TimeUnit.SECONDS)
                .readCompacted(true)
                .poolMessages(true)
                .delayAcknowledgement(true)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .createAsync()
                .thenComposeAsync(reader ->
                        compactAndCloseReader(reader, strategy), scheduler);

    }

    <T> CompletableFuture<Long> doCompaction(Reader<T> reader, TopicCompactionStrategy strategy) {

        if (!(reader instanceof DelayedAckReaderImpl<T>)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("reader has to be DelayedAckReaderImpl"));
        }
        return reader.hasMessageAvailableAsync()
                .thenCompose(available -> {
                    if (available) {
                        return phaseOne(reader, strategy)
                                .thenCompose((result) -> phaseTwo(result, reader, bk));
                    } else {
                        log.info("Skip compaction of the empty topic {}", reader.getTopic());
                        return CompletableFuture.completedFuture(-1L);
                    }
                });
    }

    <T> CompletableFuture<Long> compactAndCloseReader(Reader<T> reader, TopicCompactionStrategy strategy) {
        CompletableFuture<Long> promise = new CompletableFuture<>();
        mxBean.addCompactionStartOp(reader.getTopic());
        doCompaction(reader, strategy).whenComplete(
                (ledgerId, exception) -> {
                    log.info("Completed doCompaction ledgerId:{}", ledgerId);
                    reader.closeAsync().whenComplete((v, exception2) -> {
                        if (exception2 != null) {
                            log.warn("Error closing reader handle {}, ignoring", reader, exception2);
                        }
                        if (exception != null) {
                            // complete with original exception
                            mxBean.addCompactionEndOp(reader.getTopic(), false);
                            promise.completeExceptionally(exception);
                        } else {
                            mxBean.addCompactionEndOp(reader.getTopic(), true);
                            promise.complete(ledgerId);
                        }
                    });
                });
        return promise;
    }

    private <T> boolean doCompactMessage(Message<T> msg, PhaseOneResult<T> result, TopicCompactionStrategy strategy) {
        Map<String, Message<T>> cache = result.cache;
        String key = msg.getKey();

        if (key == null) {
            msg.release();
            return true;
        }
        T val = msg.getValue();
        Message<T> prev = cache.get(key);
        T prevVal = prev == null ? null : prev.getValue();

        if (strategy.isValid(prevVal, val)) {
            /*
            TODO: create a message by the merged val
            if (strategy.isMergeEnabled()) {
                T mergedVal = (T) strategy.merge(prevVal, val);

            }*/
            if (val != null) {
                cache.remove(key); // to reorder
                cache.put(key, msg);
            } else {
                cache.remove(key);
                msg.release();
            }

            if (prev != null) {
                prev.release();
            }
            result.validCompactionCount.incrementAndGet();
            return true;
        } else {
            msg.release();
            result.invalidCompactionCount.incrementAndGet();
            return false;
        }

    }

    private static class PhaseOneResult<T> {
        MessageId firstId;
        //MessageId to; // last undeleted messageId
        MessageId lastId; // last read messageId
        Map<String, Message<T>> cache;

        AtomicInteger invalidCompactionCount;

        AtomicInteger validCompactionCount;

        String topic;

        PhaseOneResult(String topic) {
            this.topic = topic;
            cache = new LinkedHashMap<>();
            invalidCompactionCount = new AtomicInteger();
            validCompactionCount = new AtomicInteger();
        }

        @Override
        public String toString() {
            return String.format(
                    "{Topic:%s, firstId:%s, lastId:%s, cache.size:%d, "
                            + "invalidCompactionCount:%d, validCompactionCount:%d}",
                    topic,
                    firstId != null ? firstId.toString() : "",
                    lastId != null ? lastId.toString() : "",
                    cache.size(),
                    invalidCompactionCount.get(),
                    validCompactionCount.get());
        }
    }


    private  <T> CompletableFuture<PhaseOneResult> phaseOne(Reader<T> reader, TopicCompactionStrategy strategy) {
        CompletableFuture<PhaseOneResult> promise = new CompletableFuture<>();
        PhaseOneResult<T> result = new PhaseOneResult(reader.getTopic());

        ((DelayedAckReaderImpl<T>) reader).getLastMessageIdAsync()
                .thenAccept(lastMessageId -> {
                    log.info("Commencing phase one of compaction for {}, reading to {}",
                            reader.getTopic(), lastMessageId);
                    // Each entry is processed as a whole, discard the batchIndex part deliberately.
                    MessageIdImpl lastImpl = (MessageIdImpl) lastMessageId;
                    MessageIdImpl lastEntryMessageId = new MessageIdImpl(lastImpl.getLedgerId(), lastImpl.getEntryId(),
                            lastImpl.getPartitionIndex());
                    result.lastId = lastEntryMessageId;
                    phaseOneLoop(reader, promise, result, strategy);
                }).exceptionally(ex -> {
                    promise.completeExceptionally(ex);
                    return null;
                });

        return promise;

    }

    private <T> void phaseOneLoop(Reader<T> reader, CompletableFuture<PhaseOneResult> promise,
                              PhaseOneResult<T> result, TopicCompactionStrategy strategy) {

        if (promise.isDone()) {
            return;
        }

        CompletableFuture<Message<T>> future = reader.readNextAsync();
        FutureUtil.addTimeoutHandling(future,
                phaseOneLoopReadTimeout, scheduler,
                () -> FutureUtil.createTimeoutException("Timeout", getClass(),
                        "phaseOneLoop(...)"));

        future.thenAcceptAsync(msg -> {

            MessageId id = msg.getMessageId();
            boolean completed = false;
            if (result.lastId.compareTo(id) == 0) {
                completed = true;
            }
            mxBean.addCompactionReadOp(reader.getTopic(), msg.size());
            if (doCompactMessage(msg, result, strategy)) {
                mxBean.addCompactionRemovedEvent(reader.getTopic());
            }
            //set ids in the result

            if (completed) {
                promise.complete(result);
            } else {
                phaseOneLoop(reader, promise, result, strategy);
            }
        }, scheduler).exceptionally(ex -> {
            promise.completeExceptionally(ex);
            return null;
        });

    }

    private <T> CompletableFuture<Long> phaseTwo(PhaseOneResult<T> phaseOneResult, Reader<T> reader, BookKeeper bk) {
        log.info("Completed phase one. Result:{}. ", phaseOneResult);
        Map<String, byte[]> metadata =
                LedgerMetadataUtils.buildMetadataForCompactedLedger(
                        phaseOneResult.topic, phaseOneResult.lastId.toByteArray());
        return createLedger(bk, metadata)
                .thenCompose((ledger) -> {
                    log.info(
                            "Commencing phase two of compaction for {}, from {} to {}, compacting {} keys to ledger {}",
                            phaseOneResult.topic, phaseOneResult.firstId, phaseOneResult.lastId,
                            phaseOneResult.cache.size(), ledger.getId());
                    return runPhaseTwo(phaseOneResult, reader, ledger, bk);
                });
    }

    private <T> CompletableFuture<Long> runPhaseTwo(
            PhaseOneResult<T> phaseOneResult, Reader<T> reader, LedgerHandle ledger, BookKeeper bk) {
        CompletableFuture<Long> promise = new CompletableFuture<>();
        CompletableFuture<Void> loopPromise = new CompletableFuture<>();
        Semaphore outstanding = new Semaphore(MAX_OUTSTANDING);
        phaseTwoLoop(phaseOneResult.topic, phaseOneResult.cache.values().iterator(), ledger,
                outstanding, loopPromise);
        loopPromise.thenCompose((v) -> {
                            log.info("acked ledger id {}", phaseOneResult.lastId);
                            return ((DelayedAckReaderImpl<T>) reader).acknowledgeCumulativeAsync(
                                    phaseOneResult.lastId, Map.of(COMPACTED_TOPIC_LEDGER_PROPERTY, ledger.getId()));
                        }
                )
                .thenCompose((v) -> {
                    return closeLedger(ledger);
                })
                .whenComplete((v, exception) -> {
                    if (exception != null) {
                        deleteLedger(bk, ledger).whenComplete((res2, exception2) -> {
                            if (exception2 != null) {
                                log.error("Cleanup of ledger {} for failed", ledger, exception2);
                            }
                            // complete with original exception
                            promise.completeExceptionally(exception);
                        });
                    } else {
                        log.info("kept ledger:{}", ledger.getId());
                        promise.complete(ledger.getId());
                    }
                });

        return promise;
    }

    private  <T>  void phaseTwoLoop(String topic, Iterator<Message<T>> reader,
                              LedgerHandle lh, Semaphore outstanding,
                              CompletableFuture<Void> promise) {
        if (promise.isDone()) {
            return;
        }
        CompletableFuture.runAsync(() -> {
                    if (reader.hasNext()) {
                        Message<T> message = reader.next();
                        mxBean.addCompactionReadOp(topic, message.size());
                        try {
                            outstanding.acquire();
                        } catch (InterruptedException e) {
                            promise.completeExceptionally(e);
                            return;
                        }
                        addToCompactedLedger(lh, message, topic)
                                .whenComplete((res, exception2) -> {
                                    outstanding.release();
                                    message.release();
                                    if (exception2 != null) {
                                        promise.completeExceptionally(exception2);
                                        return;
                                    }
                                });
                        phaseTwoLoop(topic, reader, lh, outstanding, promise);
                    } else {
                        try {
                            outstanding.acquire(MAX_OUTSTANDING);
                        } catch (InterruptedException e) {
                            promise.completeExceptionally(e);
                            return;
                        }
                        promise.complete(null);
                        return;
                    }

                }, scheduler)
                .exceptionally(ex -> {
                    promise.completeExceptionally(ex);
                    return null;
                });
    }

    <T> CompletableFuture<Void> addToCompactedLedger(LedgerHandle lh, Message<T> m, String topic) {
        CompletableFuture<Void> bkf = new CompletableFuture<>();
        // TODO: consider BatchMessageContainer.getCompressedBatchMetadataAndPayload
        ByteBuf serialized = messageToByteBuf(m);
        try {
            mxBean.addCompactionWriteOp(topic, m.size());
            long start = System.nanoTime();
            lh.asyncAddEntry(serialized,
                    (rc, ledger, eid, ctx) -> {
                        mxBean.addCompactionLatencyOp(topic, System.nanoTime() - start, TimeUnit.NANOSECONDS);
                        if (rc != BKException.Code.OK) {
                            bkf.completeExceptionally(BKException.create(rc));
                        } else {
                            bkf.complete(null);
                        }
                    }, null);
        } catch (Throwable t) {
            return FutureUtil.failedFuture(t);
        }
        return bkf;
    }

    public <T> ByteBuf messageToByteBuf(Message<T> message) {

        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");
        MessageImpl msg = (MessageImpl) message;
        MessageMetadata metadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        metadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        metadata.setUncompressedSize(payload.readableBytes());

        ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                metadata, payload);

        MessageIdData idData = new MessageIdData();
        MessageIdImpl id = (MessageIdImpl) msg.getMessageId();
        idData.setEntryId(id.getEntryId());
        idData.setLedgerId(id.getLedgerId());
        idData.setPartition(id.getPartitionIndex());

        RawMessageImpl rawMessage = new RawMessageImpl(idData, metadataAndPayload);



        return rawMessage.serialize();
        /*
        MessageMetadata messageMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(payload.readableBytes());

        return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
        */
    }


}
