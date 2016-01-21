package com.clearspring.analytics.stream.cardinality;


/**
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


import java.io.IOException;

import java.util.Arrays;

import com.clearspring.analytics.TestUtils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRecordinality {

    @Test
    public void testComputeCount() {
        Recordinality recordinality = new Recordinality(16);
        recordinality.offer(0);
        recordinality.offer(1);
        recordinality.offer(2);
        recordinality.offer(3);
        recordinality.offer(16);
        recordinality.offer(17);
        recordinality.offer(18);
        recordinality.offer(19);
        recordinality.offer(19);
        assertEquals(8, recordinality.cardinality());
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        Recordinality r = new Recordinality(8);
        r.offer("a");
        r.offer("b");
        r.offer("c");
        r.offer("d");
        r.offer("e");

        Recordinality r2 = (Recordinality) TestUtils.deserialize(TestUtils.serialize(r));
        assertEquals(r.cardinality(), r2.cardinality());
    }

    @Test
    public void testHighCardinality() {
        int counter = 0;
        for (int j = 0; j < 3; ++j) {
            long start = System.currentTimeMillis();
            Recordinality recordinality = new Recordinality(10);
            int size = 10000000;
            for (int i = 0; i < size; i++) {
                recordinality.offer(TestICardinality.streamElement(i));
            }
            System.out.println("time: " + (System.currentTimeMillis() - start));
            /**
             * the algorithm RECORDINALITY is expected to provide estimates
             * within σ, 2σ, 3σ of the exact count in respectively at least
             * 68%, 95% and 99% of all cases.
             */
            long estimate = recordinality.cardinality();
            double estimatedError = recordinality.estimatedStandarError();
            long permittedError = (long) (3*size*estimatedError);
            long err = Math.abs(estimate - size);

            if (err > permittedError) ++counter;

        }
        System.out.println("If counter (> 1) rerun the test. \nIf you have already done it, something is broken");
        System.out.println("Counter: " + counter);
        assertTrue(counter < 2);
    }
}
