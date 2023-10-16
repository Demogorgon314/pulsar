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
import io.netty.buffer.Unpooled;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
public class MangedLedgerInterceptorImplTest  extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(MangedLedgerInterceptorImplTest.class);

    public static class TestPayloadProcessor implements ManagedLedgerPayloadProcessor {
        @Override
        public Processor inputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] newMessage = (new String("Modified Test Message")).getBytes();
                    ByteBuf processedPayload =  Unpooled.wrappedBuffer(newMessage, 0, newMessage.length);
                    inputPayload.release();
                    return processedPayload.retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
        @Override
        public Processor outputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] bytes = new byte[inputPayload.readableBytes()];
                    inputPayload.readBytes(bytes);
                    String storedMessage = new String(bytes);
                    Assert.assertTrue(storedMessage.equals("Modified Test Message"));

                    byte[] newMessage = (new String("Test Message")).getBytes();
                    inputPayload.release();
                    return Unpooled.wrappedBuffer(newMessage, 0, newMessage.length).retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
    }

    @Test
    public void testAddBrokerEntryMetadata() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "topicEntryMetadataSequenceId";

        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), MOCK_BATCH_SIZE);
        }

        assertEquals(19, ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex());
        assertEquals(0, ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex());
        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entryList.get(i).getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), (i + 1) * MOCK_BATCH_SIZE - 1);
        }

        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    @Test
    public void testAddBrokerEntryMetadataAndTrim() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        int numberOfEntries = 10;
        // message size is 1MB
        final int messageSize = 1048576;
        char[] data = new char[messageSize];
        Arrays.fill(data, 'a');
        byte [] message = new String(data).getBytes(StandardCharsets.UTF_8);
        final String ledgerAndCursorName = "testAddBrokerEntryMetadataAndTrim";

        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setRetentionSizeInMB(5);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(message, MOCK_BATCH_SIZE);
        }

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 0);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);

        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entryList.get(i).getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), (i + 1) * MOCK_BATCH_SIZE - 1);
        }

        // Trim ledgers.
        cursor.markDelete(entryList.get(numberOfEntries - 1).getPosition());
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.trimConsumedLedgersInBackground(promise);
        promise.get();
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 16);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);

        cursor.close();
        ledger.close();

        // Re-open ledger
        ledger = factory.open(ledgerAndCursorName, config);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 16);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);

        cursor.close();
        ledger.close();
        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void testTrimmer() throws Exception {
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);
        int numberOfEntries = 4;
        String ledgerAndCursorName = "my_test_trimmer_ledger";

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.SECONDS);
        config.setManagedLedgerInterceptor(interceptor);
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        for (int i = 0; i < numberOfEntries; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8), 1);
        }
        assertEquals(ledger.getNumberOfEntries(), 4);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 0);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 3);

        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entryList.get(i).getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), i);
        }

        Position lastPosition = entryList.get(entryList.size() - 1).getPosition();
        entryList.forEach(Entry::release);

        assertEquals(ledger.getNumberOfEntries(), 4);

        cursor.markDelete(lastPosition);

        ManagedLedger finalLedger = ledger;
        Awaitility.await().until(() -> {
            log.info("ledger.getNumberOfEntries() = {}", finalLedger.getNumberOfEntries());
            return finalLedger.getNumberOfEntries() == 1;
        });

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 3);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 3);

        cursor.close();
        ledger.close();
        // Re-open ledger
        ledger = factory.open(ledgerAndCursorName, config);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getStartIndex(), 3);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 3);
        ledger.close();

        factory.shutdown();
    }

    @Test
    public void testDeletionAfterLedgerClosedAndRetention() throws Exception {
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(2);
        config.setRetentionTime(1, TimeUnit.SECONDS);
        config.setManagedLedgerInterceptor(interceptor);
        config.setMaximumRolloverTime(1, TimeUnit.SECONDS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("testCursor1");
        ManagedCursor c2 = ml.openCursor("testCursor2");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes(), 1);
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes(), 1);
        c1.skipEntries(2, ManagedCursor.IndividualDeletedEntries.Exclude);
        c2.skipEntries(2, ManagedCursor.IndividualDeletedEntries.Exclude);
        // let current ledger close
        Field stateUpdater = ManagedLedgerImpl.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(ml, ManagedLedgerImpl.State.LedgerOpened);
        ml.rollCurrentLedgerIfFull();
        // let retention expire
        Thread.sleep(1500);
        // delete the expired ledger
        CompletableFuture<Object> promise = CompletableFuture.completedFuture(null);
        ml.trimConsumedLedgersInBackground(promise);
        promise.get();

        // the closed and expired ledger should be deleted
        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertEquals(ml.getTotalSize(), 0);
        assertEquals(((ManagedLedgerInterceptorImpl) ml.getManagedLedgerInterceptor()).getStartIndex(), 2);
        assertEquals(((ManagedLedgerInterceptorImpl) ml.getManagedLedgerInterceptor()).getIndex(), 1);
        ml.close();
    }

    @Test
    public void testMessagePayloadProcessor() throws Exception {
        final String ledgerAndCursorName = "topicEntryWithPayloadProcessed";

        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(),processors);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        ledger.addEntry("Test Message".getBytes());
        factory.getEntryCacheManager().clear();
        List<Entry> entryList = cursor.readEntries(1);
        String message = new String(entryList.get(0).getData());
        Assert.assertTrue(message.equals("Test Message"));
        cursor.close();
        ledger.close();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }

    @Test
    public void testTotalSizeCorrectIfHasInterceptor() throws Exception {
        final String mlName = "ml1";
        final String cursorName = "cursor1";

        // Registry interceptor.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(), processors);
        config.setManagedLedgerInterceptor(interceptor);
        config.setMaxEntriesPerLedger(2);

        // Add many entries and consume.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(mlName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
        for (int i = 0; i < 5; i++){
            cursor.delete(ledger.addEntry(new byte[1]));
        }

        // Trim ledgers.
        CompletableFuture<Void> trimLedgerFuture = new CompletableFuture<>();
        ledger.trimConsumedLedgersInBackground(trimLedgerFuture);
        trimLedgerFuture.join();

        // verify.
        assertEquals(ledger.getTotalSize(), calculatePreciseSize(ledger));

        // cleanup.
        cursor.close();
        ledger.close();
        factory.getEntryCacheManager().clear();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }

    public static long calculatePreciseSize(ManagedLedgerImpl ledger){
        return ledger.getLedgersInfo().values().stream()
                .map(info -> info.getSize()).reduce((l1,l2) -> l1 + l2).orElse(0L) + ledger.getCurrentLedgerSize();
    }

    @Test(timeOut = 20000)
    public void testRecoveryIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerInterceptor(interceptor);
        ManagedLedger ledger = factory.open("my_recovery_index_test_ledger", config);

        ledger.addEntry("dummy-entry-1".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE);

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_recovery_index_test_ledger", config);

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), MOCK_BATCH_SIZE * 2 - 1);


        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        cursor.close();
        ledger.close();
    }

    @Test
    public void testFindPositionByIndex() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final int maxEntriesPerLedger = 5;
        int maxSequenceIdPerLedger = MOCK_BATCH_SIZE * maxEntriesPerLedger;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);


        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setManagedLedgerInterceptor(interceptor);
        managedLedgerConfig.setMaxEntriesPerLedger(5);

        ManagedLedger ledger = factory.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);
        ManagedCursor cursor = ledger.openCursor("c1");

        long firstLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            firstLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 9);


        PositionImpl position = null;
        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // roll over ledger
        long secondLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            secondLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }

        // reopen ledger
        ledger.close();
        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);

        long thirdLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            thirdLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), MOCK_BATCH_SIZE).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / MOCK_BATCH_SIZE);
        }
        cursor.close();
        ledger.close();
    }

    @Test
    public void testAddEntryFailed() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final String ledgerAndCursorName = "testAddEntryFailed";

        ManagedLedgerInterceptor interceptor =
                new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ByteBuf buffer = Unpooled.wrappedBuffer("message".getBytes());
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);

        ledger.terminate();

        ManagedLedgerInterceptorImpl interceptor1 =
                (ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ledger.asyncAddEntry(buffer, MOCK_BATCH_SIZE, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    countDownLatch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    countDownLatch.countDown();
                }
            }, null);

            countDownLatch.await();
            assertEquals(interceptor1.getIndex(), -1);
        } finally {
            ledger.close();
            factory.shutdown();
        }

    }

    @Test
    public void testBeforeAddEntryWithException() throws Exception {
        final int MOCK_BATCH_SIZE = 2;
        final String ledgerAndCursorName = "testBeforeAddEntryWithException";

        ManagedLedgerInterceptor interceptor =
                new MockManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ByteBuf buffer = Unpooled.wrappedBuffer("message".getBytes());
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ledger.asyncAddEntry(buffer, MOCK_BATCH_SIZE, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    countDownLatch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    countDownLatch.countDown();
                }
            }, null);
            countDownLatch.await();
            assertEquals(buffer.refCnt(), 1);
        } finally {
            ledger.close();
            factory.shutdown();
        }
    }

    private class MockManagedLedgerInterceptorImpl extends ManagedLedgerInterceptorImpl {
        private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

        public MockManagedLedgerInterceptorImpl(
                Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
            super(brokerEntryMetadataInterceptors, brokerEntryPayloadProcessors);
            this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
        }

        @Override
        public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
            if (op == null || numberOfMessages <= 0) {
                return op;
            }
            op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors,
                    numberOfMessages));
            if (op != null) {
                throw new RuntimeException("throw exception before add entry for test");
            }
            return op;
        }
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }

    static class IndexSearchPredicate implements Predicate<Entry> {

        long indexToSearch = -1;
        public IndexSearchPredicate(long indexToSearch) {
            this.indexToSearch = indexToSearch;
        }

        @Override
        public boolean test(@Nullable Entry entry) {
            try {
                BrokerEntryMetadata brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                return brokerEntryMetadata.getIndex() < indexToSearch;
            } catch (Exception e) {
                log.error("Error deserialize message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }
    }

}
