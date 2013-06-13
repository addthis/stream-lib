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

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Eric Vlaanderen
 */
public class ErrorAndCount<T>
{
	protected final AtomicLong error;
	protected final AtomicLong count;
	protected final T value;

	public ErrorAndCount(final T value, final long count, final long error)
	{
		this.value = value;
		this.error = new AtomicLong(error);
		this.count = new AtomicLong(count);
	}

	public ErrorAndCount(final T value, final long count)
	{
		 this(value, count, 0L);
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Value: ");
		sb.append(value);
		sb.append(", Count: ");
		sb.append(count);
		sb.append(", Error: ");
		sb.append(error);
		sb.append(", object: ");
		sb.append(super.toString());
		return sb.toString();
	}
}
