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

package com.clearspring.analytics.stream.cardinality;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class TestICardinality {

    private int N = 1000000;
    private ICardinality cardinalityEstimator;
    private static Random prng = new Random();
    private static char[] hex = "0123456789abcdef".toCharArray();

    public TestICardinality(ICardinality cardinalityEstimator) {
        super();
        this.cardinalityEstimator = cardinalityEstimator;
    }

    @Test
    public void testOffer() {
        cardinalityEstimator.offer("A");
        cardinalityEstimator.offer("B");
        cardinalityEstimator.offer("C");
        assertFalse(cardinalityEstimator.offer("C"));
        assertFalse(cardinalityEstimator.offer("B"));
        assertFalse(cardinalityEstimator.offer("A"));
        cardinalityEstimator.offer("ABCCBA");
        cardinalityEstimator.offer("CBAABC");
        cardinalityEstimator.offer("ABCABC");
        cardinalityEstimator.offer("CBACBA");
        assertFalse(cardinalityEstimator.offer("ABCCBA"));
    }

    @Test
    public void testICardinality() {
        System.out.println("size: " + cardinalityEstimator.sizeof() + " bytes");
        for (int i = 0; i < N; i++) {
            cardinalityEstimator.offer(streamElement(i));
        }

        long estimate = cardinalityEstimator.cardinality();
        System.out.println(estimate);
        double err = Math.abs(estimate - N) / (double) N;
        System.out.println("% Error: " + err * 100);
    }

    static int se = 0;

    public static Object streamElement(int i) {
        return Long.toHexString(prng.nextLong());
        //return se++;
    }

    @Parameters
    public static Collection<Object[]> regExValues() {
        return Arrays.asList(new Object[][]{
                //{ new LinearCounting(65536) }, 
                //{ new CountThenEstimate() },
                {new AdaptiveCounting(16)},
                //{ new LogLog(10) },
                //{ new LogLog(12) }, 
                //{ new LogLog(14) },
        });
    }

}
