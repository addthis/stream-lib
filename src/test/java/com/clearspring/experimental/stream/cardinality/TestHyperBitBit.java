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

package com.clearspring.experimental.stream.cardinality;

import com.clearspring.analytics.stream.cardinality.TestICardinality;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertTrue;


public class TestHyperBitBit {
    // This test are "in progress" until HyperBitBit has better known error bounds
    // Right now they will no fail ever (@ignore)


    @Test
    @Ignore
    public void testSimpleHighCardinality() {
        int size = 10000000;
        int max_repetitions = 5;
        double errors_mean = 0;
        for (int repetitions = 0; repetitions < max_repetitions; ++repetitions) {
            long start = System.currentTimeMillis();
            HyperBitBit hyperBitBit = new HyperBitBit();

            for (int i = 0; i < size; i++) {
                hyperBitBit.offer(TestICardinality.streamElement(i));
            }
            System.out.println("time: " + (System.currentTimeMillis() - start));
            long estimate = hyperBitBit.cardinality();
            double err = Math.abs(estimate - size) / (double) size;
            errors_mean += (err/max_repetitions);
            System.out.println(err);
        }
        System.out.println("This value should be less than 0.25: " + errors_mean);
        assertTrue(errors_mean < 0.1);

    }

    @Test
    @Ignore
    public void testMultipleOrderedHighCardinality() {
        int size = 10000000;

        long start = System.currentTimeMillis();

        HyperBitBit hyperBitBit = new HyperBitBit();

        for (int i = 0; i < size; i++) {
            hyperBitBit.offer(i);
            hyperBitBit.offer(i);
            hyperBitBit.offer(i);
            hyperBitBit.offer(i);
        }

        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = hyperBitBit.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        System.out.println("This value should be less than 0.2: " + err);
        assertTrue(err < 0.1);
    }

    @Test
    @Ignore
    public void testMultipleUnorderedHighCardinality() {
        int size = 10000000;

        long start = System.currentTimeMillis();

        HyperBitBit hyperBitBit = new HyperBitBit();

        for (int i = 0; i < size; i++) {
            hyperBitBit.offer(i);
        }

        for (int i = 0; i < size; i++) {
            hyperBitBit.offer(i);
        }

        for (int i = 0; i < size; i++) {
            hyperBitBit.offer(i);
        }

        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = hyperBitBit.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);

        System.out.println("This value should be less than 0.2: " + err);
        assertTrue(err < 0.1);
    }
}
