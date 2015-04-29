/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
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

package com.clearspring.analytics.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 * <p/>
 * Ideally used in multithreaded applications, otherwise see {@link StreamSummary}
 *
 * @param <T> type of data in the stream to be summarized
 * @author Eric Vlaanderen
 */
public class ConcurrentStreamSummary<T> implements ITopK<T> {

    private final int capacity;
    private final ConcurrentHashMap<T, ScoredItem<T>> itemMap;
    private final AtomicReference<ScoredItem<T>> minVal;
    private final AtomicLong size;
    private final AtomicBoolean reachCapacity;

    public ConcurrentStreamSummary(final int capacity) {
        this.capacity = capacity;
        this.minVal = new AtomicReference<ScoredItem<T>>();
        this.size = new AtomicLong(0);
        this.itemMap = new ConcurrentHashMap<T, ScoredItem<T>>(capacity);
        this.reachCapacity = new AtomicBoolean(false);
    }

    @Override
    public boolean offer(final T element) {
        return offer(element, 1);
    }

    @Override
    public boolean offer(final T element, final int incrementCount) {
        long val = incrementCount;
        ScoredItem<T> value = new ScoredItem<T>(element, incrementCount);
        ScoredItem<T> oldVal = itemMap.putIfAbsent(element, value);
        if (oldVal != null) {
            val = oldVal.addAndGetCount(incrementCount);
        } else if (reachCapacity.get() || size.incrementAndGet() > capacity) {
            reachCapacity.set(true);

            ScoredItem<T> oldMinVal = minVal.getAndSet(value);
            itemMap.remove(oldMinVal.getItem());

            while (oldMinVal.isNewItem()) {
                // Wait for the oldMinVal so its error and value are completely up to date.
                // no thread.sleep here due to the overhead of calling it - the waiting time will be microseconds.
            }
            long count = oldMinVal.getCount();

            value.addAndGetCount(count);
            value.setError(count);
        }
        value.setNewItem(false);
        minVal.set(getMinValue());

        return val != incrementCount;
    }

    private ScoredItem<T> getMinValue() {
        ScoredItem<T> minVal = null;
        for (ScoredItem<T> entry : itemMap.values()) {
            if (minVal == null || (!entry.isNewItem() && entry.getCount() < minVal.getCount())) {
                minVal = entry;
            }
        }
        return minVal;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ScoredItem entry : itemMap.values()) {
            sb.append("(" + entry.getCount() + ": " + entry.getItem() + ", e: " + entry.getError() + "),");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<T> peek(final int k) {
        List<T> toReturn = new ArrayList<T>(k);
        List<ScoredItem<T>> values = peekWithScores(k);
        for (ScoredItem<T> value : values) {
            toReturn.add(value.getItem());
        }
        return toReturn;
    }

    public List<ScoredItem<T>> peekWithScores(final int k) {
        List<ScoredItem<T>> values = new ArrayList<ScoredItem<T>>();
        for (Map.Entry<T, ScoredItem<T>> entry : itemMap.entrySet()) {
            ScoredItem<T> value = entry.getValue();
            values.add(new ScoredItem<T>(value.getItem(), value.getCount(), value.getError()));
        }
        Collections.sort(values);
        values = values.size() > k ? values.subList(0, k) : values;
        return values;
    }
}
