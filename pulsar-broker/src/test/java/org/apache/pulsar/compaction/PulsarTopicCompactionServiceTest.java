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
package org.apache.pulsar.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PulsarTopicCompactionService#readCompactedEntries}.
 *
 * <p>The compacted ledger below keeps 7 entries whose original message ids contain gaps (ids removed by
 * compaction), mirroring a {@code __change_events} topic whose original ledger has been fully compacted
 * and trimmed. A reader resuming from a position inside such a gap must either receive the remaining
 * compacted entries or observe a failed read that it can retry; receiving an empty result makes
 * {@link CompactedTopicUtils#asyncReadCompactedEntries} seek the cursor past the whole compacted range,
 * silently skipping the remaining entries.
 */
@Test(groups = "broker-compaction")
public class PulsarTopicCompactionServiceTest {

    private static final String TOPIC = "persistent://tenant/ns/my-topic";
    private static final long ORIGINAL_LEDGER_ID = 4658112L;
    private static final long COMPACTED_LEDGER_ID = 4658135L;
    // Original message entry ids of the entries kept in the compacted ledger, indexed by compacted entry id.
    private static final long[] MESSAGE_ENTRY_IDS = {2, 4, 6, 7, 9, 11, 13};

    @Test
    public void testReadFromPositionInsideCompactedRange() throws Exception {
        PulsarTopicCompactionService service = newServiceWithCompactedLedger(Set.of());

        List<Entry> entries = service.readCompactedEntries(PositionFactory.create(ORIGINAL_LEDGER_ID, 3), 10)
                .get(10, TimeUnit.SECONDS);

        assertEquals(entries.size(), 6);
        assertEquals(entries.get(0).getPosition(), PositionFactory.create(ORIGINAL_LEDGER_ID, 4));
        assertEquals(entries.get(entries.size() - 1).getPosition(), PositionFactory.create(ORIGINAL_LEDGER_ID, 13));
        entries.forEach(Entry::release);
    }

    @Test
    public void testReadFromPositionNewerThanCompactedRange() throws Exception {
        PulsarTopicCompactionService service = newServiceWithCompactedLedger(Set.of());

        List<Entry> entries = service.readCompactedEntries(PositionFactory.create(ORIGINAL_LEDGER_ID, 14), 10)
                .get(10, TimeUnit.SECONDS);

        assertTrue(entries.isEmpty());
    }

    /**
     * A bookie read that completes OK without returning the requested index entry surfaces as a
     * {@link NoSuchElementException} from the compacted ledger's message-id index. The read must fail so the
     * dispatcher retries from the same position. Completing with an empty list instead makes the dispatcher
     * seek the cursor past the remaining compacted entries: a reader of a fully compacted topic then hangs
     * forever with {@code hasMessageAvailable() == true} but {@code readNext()} never completing, which in
     * the case of the {@code __change_events} reader blocks the namespace's topic policies initialization
     * and fails every topic load in the namespace until the broker restarts.
     */
    @Test
    public void testIndexReadFailureFailsTheRead() throws Exception {
        PulsarTopicCompactionService service = newServiceWithCompactedLedger(Set.of(1L, 2L));

        ExecutionException e = expectThrows(ExecutionException.class,
                () -> service.readCompactedEntries(PositionFactory.create(ORIGINAL_LEDGER_ID, 3), 10)
                        .get(10, TimeUnit.SECONDS));

        assertTrue(e.getCause() instanceof NoSuchElementException,
                "expected the index read failure to propagate, got: " + e.getCause());
    }

    private static PulsarTopicCompactionService newServiceWithCompactedLedger(Set<Long> entriesMissingFromReads)
            throws Exception {
        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        doReturn(COMPACTED_LEDGER_ID).when(ledgerHandle).getId();
        doReturn((long) (MESSAGE_ENTRY_IDS.length - 1)).when(ledgerHandle).getLastAddConfirmed();
        doAnswer(invocation -> {
            long firstEntry = invocation.getArgument(0);
            long lastEntry = invocation.getArgument(1);
            AsyncCallback.ReadCallback callback = invocation.getArgument(2);
            Object ctx = invocation.getArgument(3);
            List<LedgerEntry> entries = new ArrayList<>();
            for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                if (!entriesMissingFromReads.contains(entryId)) {
                    entries.add(newLedgerEntry(entryId));
                }
            }
            callback.readComplete(BKException.Code.OK, ledgerHandle, Collections.enumeration(entries), ctx);
            return null;
        }).when(ledgerHandle).asyncReadEntries(anyLong(), anyLong(), any(), any());

        BookKeeper bookKeeper = mock(BookKeeper.class);
        doAnswer(invocation -> {
            AsyncCallback.OpenCallback callback = invocation.getArgument(3);
            callback.openComplete(BKException.Code.OK, ledgerHandle, invocation.getArgument(4));
            return null;
        }).when(bookKeeper).asyncOpenLedger(eq(COMPACTED_LEDGER_ID), any(), any(), any(), any(), anyBoolean());

        PulsarTopicCompactionService service = new PulsarTopicCompactionService(TOPIC, bookKeeper, () -> null);
        service.getCompactedTopic()
                .newCompactedLedger(PositionFactory.create(ORIGINAL_LEDGER_ID + 1, -1), COMPACTED_LEDGER_ID)
                .get(10, TimeUnit.SECONDS);
        return service;
    }

    private static LedgerEntry newLedgerEntry(long entryId) {
        MessageIdData messageIdData = new MessageIdData();
        messageIdData.setLedgerId(ORIGINAL_LEDGER_ID);
        messageIdData.setEntryId(MESSAGE_ENTRY_IDS[(int) entryId]);
        ByteBuf headersAndPayload = Unpooled.wrappedBuffer("payload".getBytes(StandardCharsets.UTF_8));
        RawMessageImpl rawMessage = new RawMessageImpl(messageIdData, headersAndPayload);
        ByteBuf serialized = rawMessage.serialize();
        rawMessage.close();
        LedgerEntry ledgerEntry = mock(LedgerEntry.class);
        doReturn(serialized).when(ledgerEntry).getEntryBuffer();
        return ledgerEntry;
    }
}
