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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 * 
 * Ideally used in multithreaded applications, otherwise see {@link StreamSummary}
 *
 * @param <T> type of data in the stream to be summarized
 * @author Eric Vlaanderen
 */
public class ConcurrentStreamSummary<T> implements ITopK<T>
{
	private final int capacity;
	private final ConcurrentHashMap<T, ErrorAndCount> itemMap;
	private final AtomicReference<ErrorAndCount> minVal = new AtomicReference<>();
	private final AtomicLong size = new AtomicLong(0);

	public ConcurrentStreamSummary(final int capacity)
	{
		this.capacity = capacity;
		itemMap = new ConcurrentHashMap<>(capacity);
	}

	@Override
	public boolean offer(final T element)
	{
		return offer(element, 1);
	}

	@Override
	public boolean offer(final T element, final int incrementCount)
	{
		long val = incrementCount;
		ErrorAndCount value = new ErrorAndCount(element, incrementCount);
		ErrorAndCount oldVal = itemMap.putIfAbsent(element, value);
		if (oldVal != null)
		{
			val = oldVal.count.addAndGet(incrementCount);
		}
		else if (size.incrementAndGet() > capacity)
		{
			ErrorAndCount oldMinVal = minVal.getAndSet(value);
			long count = oldMinVal.count.get();
			itemMap.remove(oldMinVal.value);

			value.count.getAndAdd(count);
			value.error.set(count);
		}
		minVal.set(getMinValue());

		return val != incrementCount;
	}

	private ErrorAndCount getMinValue()
	{
		ErrorAndCount<T> minVal = null;
		for (ErrorAndCount<T> entry : itemMap.values())
		{
			if (minVal == null || entry.count.get() < minVal.count.get())
			{
				minVal = entry;
			}
		}
		return minVal;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (ErrorAndCount entry : itemMap.values())
		{
			sb.append("("+ entry.count.get()  + ": " + entry.value + ", e: " + entry.error.get() + "),");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]");
		return sb.toString();
	}

	@Override
	public List<T> peek(final int k)
	{
		List<T> toReturn = new ArrayList<>(k);
		List<ScoredItem<T>> values = peekWithScores(k);
		for (ScoredItem<T> value : values)
		{
			toReturn.add(value.getItem());
		}
		return toReturn;
	}

	public List<ScoredItem<T>> peekWithScores(final int k)
	{
		List<ScoredItem<T>> values = new ArrayList<>();
		for (Map.Entry<T, ErrorAndCount> entry : itemMap.entrySet())
		{
			values.add(new ScoredItem(entry.getKey(), entry.getValue().count.get(), entry.getValue().error.get()));
		}
		Collections.sort(values);
		values = values.size() > k ? values.subList(0, k) : values;
		return values;
	}
}
