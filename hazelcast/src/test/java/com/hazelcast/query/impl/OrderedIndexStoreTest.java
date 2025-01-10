/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OrderedIndexStoreTest {
    OrderedIndexStore store = new OrderedIndexStore(IndexCopyBehavior.COPY_ON_WRITE);
    int size = 9;

    Function<Integer, Integer> keyToIndex = (Integer i) -> i % 3;

    @Before
    public void setup() {
        range(0, size).forEach(i -> store.insertInternal(keyToIndex.apply(i), new DummyEntry(i, keyToIndex.apply(i))));
    }


    @Test
    public void getSqlRecordIteratorBatchLeftIncludedRightIncludedDescending() {
        List<Integer> expectedKeyOrder = Arrays.asList(7, 4, 1, 6, 3, 0);
        Iterator<IndexKeyEntries> result = store.getSqlRecordIteratorBatch(0, true, 1, true, true);
        assertResult(expectedKeyOrder, result);
    }

    @Test
    public void getSqlRecordIteratorBatchLeftExcludeRightExcludeAscending() {
        List<Integer> expectedKeyOrder = Arrays.asList(1, 4, 7);
        Iterator<IndexKeyEntries> result = store.getSqlRecordIteratorBatch(0, false, 2, false, false);
        assertResult(expectedKeyOrder, result);
    }

    @Test
    public void getSqlRecordIteratorBatchLeftIncludedRightExcludedDescending() {
        List<Integer> expectedKeyOrder = Arrays.asList(6, 3, 0);
        Iterator<IndexKeyEntries> result = store.getSqlRecordIteratorBatch(0, true, 1, false, true);
        assertResult(expectedKeyOrder, result);
    }

    @Test
    public void getSqlRecordIteratorBatchLeftExcludedRightIncludedAscending() {
        List<Integer> expectedKeyOrder = Arrays.asList(1, 4, 7);
        Iterator<IndexKeyEntries> result = store.getSqlRecordIteratorBatch(0, false, 1, true, false);
        assertResult(expectedKeyOrder, result);
    }

    @Test
    public void getSqlRecordIteratorBatchCursorLeftIncludeRightIncludedAscending() {
        List<Integer> expectedOrder = Arrays.asList(0, 3, 6, 1, 4, 7);
        performCursorTest(3, expectedOrder, cursor -> store.getSqlRecordIteratorBatch(0, true, 1, true, false, cursor));
    }

    @Test
    public void getSqlRecordIteratorBatchCursorLeftExcludedRightIncludedDescending() {
        List<Integer> expectedOrder = Arrays.asList(7, 4, 1);
        performCursorTest(expectedOrder, cursor -> store.getSqlRecordIteratorBatch(0, false, 1, true, true, cursor));
    }


    @Test
    public void getSqlRecordIteratorBatchCursorLeftIncludedAscending() {
        List<Integer> expectedOrder = Arrays.asList(0, 3, 6, 1, 4, 7);
        performCursorTest(3, expectedOrder, cursor -> store.getSqlRecordIteratorBatch(0, true, 2, false, false, cursor));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSqlRecordIteratorBatchCursorLeftExcludedRightExcluded() {
        store.getSqlRecordIteratorBatch(0, false, 1, false, true, buildCursor(0));
    }

    @Test
    public void getRecordAllAscending() {
        List<Integer> expectedOrder = Arrays.asList(0, 3, 6, 1, 4, 7, 2, 5, 8);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(false);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordAllDescending() {
        List<Integer> expectedOrder = Arrays.asList(8, 5, 2, 7, 4, 1, 6, 3, 0);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(true);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordsUsingExactValueAscending() {
        List<Integer> expectedOrder = Arrays.asList(1, 4, 7);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(1, false);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordsUsingExactValueDescending() {
        List<Integer> expectedOrder = Arrays.asList(7, 4, 1);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(1, true);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordsWithCursorUsingExactValueAscending() {
        List<Integer> expectedOrder = Arrays.asList(1, 4, 7);
        performCursorTest(expectedOrder, cursor -> store.getSqlRecordIteratorBatch(1, false, cursor));
    }

    @Test
    public void getRecordsWithCursorUsingExactValueDepending() {
        List<Integer> expectedOrder = Arrays.asList(7, 4, 1);
        performCursorTest(expectedOrder, cursor -> store.getSqlRecordIteratorBatch(1, true, cursor));
    }

    @Test
    public void getRecordsUsingExactValueInequalityAscending() {
        List<Integer> expectedOrder = Arrays.asList(1, 4, 7, 2, 5, 8);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(Comparison.GREATER, 0, false);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordsUsingExactValueInequalityDescending() {
        List<Integer> expectedOrder = Arrays.asList(7, 4, 1, 6, 3, 0);
        Iterator<IndexKeyEntries> actual = store.getSqlRecordIteratorBatch(Comparison.LESS_OR_EQUAL, 1, true);
        assertResult(expectedOrder, actual);
    }

    @Test
    public void getRecordsWithCursorUsingExactValueInequalityAscending() {
        List<Integer> expectedOrder = Arrays.asList(1, 4, 7, 2, 5, 8);
        performCursorTest(3, expectedOrder, cursor -> store.getSqlRecordIteratorBatch(Comparison.GREATER_OR_EQUAL, 1, false, cursor));
    }

    @Test
    public void getRecordsWithCursorUsingExactValueInequalityDescending() {
        List<Integer> expectedOrder = Arrays.asList(6, 3, 0);
        performCursorTest(expectedOrder, cursor -> store.getSqlRecordIteratorBatch(Comparison.LESS_OR_EQUAL, 0, true, cursor));
    }

    private Data buildCursor(int key) {
        return new HeapData(ByteBuffer.allocate(8).putInt(key).array());
    }

    private void performCursorTest(List<Integer> order, Function<Data, Iterator<IndexKeyEntries>> cursorIteratorFunction) {
        performCursorTest(order.size(), order, cursorIteratorFunction);
    }

    private void performCursorTest(int cursorCases, List<Integer> order, Function<Data, Iterator<IndexKeyEntries>> cursorIteratorFunction) {
        for (int i = 0; i < cursorCases; i++) {
            Data cursor = buildCursor(order.get(i));
            Iterator<IndexKeyEntries> result = cursorIteratorFunction.apply(cursor);
            assertResult(order.subList(i + 1, order.size()), result);
        }
    }

    private void assertResult(List<Integer> expected, Iterator<IndexKeyEntries> actual) {
        Iterator<Integer> expectedKeyOrder = expected.iterator();
        while (actual.hasNext()) {
            Iterator<QueryableEntry> entries = actual.next().getEntries();
            while (entries.hasNext()) {
                assertEquals(entries.next().getKey(), expectedKeyOrder.next());
            }
        }
        assertFalse(expectedKeyOrder.hasNext());
    }

    private static class DummyEntry extends QueryEntry {
        Integer key;
        int value;
        Data keyData;

        DummyEntry(int key, int value) {
            this.key = key;
            this.value = value;
            this.keyData = new HeapData(ByteBuffer.allocate(8).putInt(key).array());
        }

        @Override
        public Data getKeyData() {
            return keyData;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            if (!super.equals(object)) {
                return false;
            }
            DummyEntry that = (DummyEntry) object;
            return value == that.value && Objects.equals(key, that.key) && Objects.equals(keyData, that.keyData);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), key, value, keyData);
        }
    }
}
