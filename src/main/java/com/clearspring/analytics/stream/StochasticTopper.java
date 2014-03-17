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

/**
 *
 */
package com.clearspring.analytics.stream;

import java.util.List;
import java.util.Random;

/**
 * Estimates most frequently occurring items in a data stream
 * using a bounded amount of memory.
 * <p/>
 * Warning: this class is not thread safe.
 */
public class StochasticTopper<T> implements ITopK<T> {

    private int sampleSize;
    private ISampleSet<T> sample;
    private Random random;
    private long count;

    public StochasticTopper(int sampleSize) {
        this(sampleSize, null);
    }

    public StochasticTopper(int sampleSize, Long seed) {
        this.sample = new SampleSet<T>(sampleSize);
        this.sampleSize = sampleSize;

        if (seed != null) {
            random = new Random(seed);
        } else {
            random = new Random();
        }
    }

    public boolean offer(T item, int incrementCount) {
        count++;
        boolean taken = false;
        if (sample.count() < sampleSize) {
            sample.put(item, incrementCount);
            taken = true;
        } else if (random.nextDouble() < sampleSize / (double) count) {
            sample.removeRandom();
            sample.put(item, incrementCount);
            taken = true;
        }

        return taken;
    }

    public boolean offer(T item) {
        return offer(item, 1);
    }

    /**
     * Retrieve top k items
     */
    public List<T> peek(int k) {
        return sample.peek(k);
    }
}
