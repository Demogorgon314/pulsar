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

package org.apache.pulsar.client.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

@Slf4j
public class TableViewImpl<T> implements TableView<T> {

    private final TableViewConfigurationData conf;

    private final Map<String, T> immutableData;

    private final Cache<String, T> cache;

    private final CompletableFuture<Reader<T>> reader;

    private final List<BiConsumer<String, T>> listeners;
    private final ReentrantLock listenersMutex;

    private final boolean isNonPersistentTopic;

    private TopicCompactionStrategy<T> compactionStrategy;

    TableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        this.conf = conf;
        Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder();
        this.isNonPersistentTopic = conf.getTopicName().startsWith(TopicDomain.non_persistent.toString());
        if (isNonPersistentTopic && conf.getTtl() > 0) {
            caffeineBuilder.expireAfterWrite(conf.getTtl(), TimeUnit.NANOSECONDS);
        }
        this.cache = caffeineBuilder.build();
        this.immutableData = Collections.unmodifiableMap(cache.asMap());
        this.listeners = new ArrayList<>();
        this.listenersMutex = new ReentrantLock();
        this.compactionStrategy = TopicCompactionStrategy.load(conf.getTopicCompactionStrategy());
        if (isNonPersistentTopic) {
            this.reader = client.newReader(schema)
                    .topic(conf.getTopicName())
                    .startMessageId(MessageId.latest)
                    .readerListener((ReaderListener<T>) (reader, msg) -> handleMessage(msg))
                    .createAsync();
            return;
        }
        this.reader = client.newReader(schema)
                .topic(conf.getTopicName())
                .startMessageId(MessageId.earliest)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval((int) conf.getAutoUpdatePartitionsSeconds(), TimeUnit.SECONDS)
                .readCompacted(true)
                .poolMessages(true)
                .createAsync();
    }

    CompletableFuture<TableView<T>> start() {
        if (isNonPersistentTopic) {
            return reader.thenApply(__ -> this);
        }
        return reader.thenCompose(this::readAllExistingMessages)
                .thenApply(__ -> this);
    }

    @Override
    public int size() {
        return cache.asMap().size();
    }

    @Override
    public boolean isEmpty() {
        return cache.asMap().isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return cache.asMap().containsKey(key);
    }

    @Override
    public T get(String key) {
       return cache.asMap().get(key);
    }

    @Override
    public Set<Map.Entry<String, T>> entrySet() {
       return immutableData.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return immutableData.keySet();
    }

    @Override
    public Collection<T> values() {
        return immutableData.values();
    }

    @Override
    public void forEach(BiConsumer<String, T> action) {
        cache.asMap().forEach(action);
    }

    @Override
    public void forEachAndListen(BiConsumer<String, T> action) {
        // Ensure we iterate over all the existing entry _and_ start the listening from the exact next message
        try {
            listenersMutex.lock();

            // Execute the action over existing entries
            forEach(action);

            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public void listen(BiConsumer<String, T> action) {
        try {
            listenersMutex.lock();
            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return reader.thenCompose(Reader::closeAsync);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    private void handleMessage(Message<T> msg) {
        try {
            if (msg.hasKey()) {

                try {
                    listenersMutex.lock();
                    String key = msg.getKey();
                    T cur = msg.size() > 0 ? msg.getValue() : null;
                    if (log.isDebugEnabled()) {
                        log.info("Applying message from topic {}. key={} value={}, messageId:{}",
                                conf.getTopicName(),
                                key,
                                cur,
                                msg.getMessageId());
                    }
                    T prev = cache.getIfPresent(key);

                    boolean skip = false;
                    if (compactionStrategy != null) {
                        skip = !compactionStrategy.isValid(prev, cur);
                        if (!skip && compactionStrategy.isMergeEnabled()) {
                            cur = compactionStrategy.merge(prev, cur);
                        }
                    }

                    if (!skip) {
                        if (null == cur) {
                            cache.invalidate(key);
                        } else {
                            cache.put(key, cur);
                        }

                        for (BiConsumer<String, T> listener : listeners) {
                            try {
                                listener.accept(key, cur);
                            } catch (Throwable t) {
                                log.error("Table view listener raised an exception", t);
                            }
                        }
                    }
                } finally {
                    listenersMutex.unlock();
                }
            }
        } finally {
            msg.release();
        }
    }

    private CompletableFuture<Reader<T>> readAllExistingMessages(Reader<T> reader) {
        long startTime = System.nanoTime();
        AtomicLong messagesRead = new AtomicLong();

        CompletableFuture<Reader<T>> future = new CompletableFuture<>();
        readAllExistingMessages(reader, future, startTime, messagesRead);
        return future;
    }

    private void readAllExistingMessages(Reader<T> reader, CompletableFuture<Reader<T>> future, long startTime,
                                         AtomicLong messagesRead) {
        reader.hasMessageAvailableAsync()
                .thenAccept(hasMessage -> {
                   if (hasMessage) {
                       reader.readNextAsync()
                               .thenAccept(msg -> {
                                  messagesRead.incrementAndGet();
                                  handleMessage(msg);
                                  readAllExistingMessages(reader, future, startTime, messagesRead);
                               }).exceptionally(ex -> {
                                   future.completeExceptionally(ex);
                                   logException(ex, reader);
                                   return null;
                               });
                   } else {
                       // Reached the end
                       long endTime = System.nanoTime();
                       long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                       log.info("Started table view for topic {} - Replayed {} messages in {} seconds",
                               reader.getTopic(),
                               messagesRead,
                               durationMillis / 1000.0);
                       future.complete(reader);
                       readTailMessages(reader);
                   }
                });
    }

    private void readTailMessages(Reader<T> reader) {
        reader.readNextAsync()
                .thenAccept(msg -> {
                    handleMessage(msg);
                    readTailMessages(reader);
                }).exceptionally(ex -> {
                    logException(ex, reader);
                    return null;
                });
    }

    private void logException(Throwable ex, Reader<T> reader) {
        if (ex.getCause() instanceof AlreadyClosedException) {
            log.warn("Reader {} was interrupted. {}", reader.getTopic(), ex.getMessage());
        } else {
            log.error("Reader {} was interrupted.", reader.getTopic(), ex);
        }
    }
}
