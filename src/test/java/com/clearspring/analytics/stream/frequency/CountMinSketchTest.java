/*
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

package com.clearspring.analytics.stream.frequency;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import com.clearspring.analytics.stream.frequency.CountMinSketch.CMSMergeException;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CountMinSketchTest {

    @Test
    public void testAccuracy() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int[] xs = new int[numItems];
        int maxScale = 20;
        for (int i = 0; i < numItems; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = r.nextInt(1 << scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (int x : xs) {
            sketch.add(x, 1);
        }

        int[] actualFreq = new int[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        sketch = CountMinSketch.deserialize(CountMinSketch.serialize(sketch));

        int numErrors = 0;
        for (int i = 0; i < actualFreq.length; ++i) {
            double ratio = ((double) (sketch.estimateCount(i) - actualFreq[i])) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }
        double pCorrect = 1.0 - ((double) numErrors) / actualFreq.length;
        assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
    }

    @Test
    public void testAccuracyStrings() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int absentItems = numItems * 10;
        String[] xs = new String[numItems];
        int maxScale = 20;
        for (int i = 0; i < numItems; i++) {
            int scale = r.nextInt(maxScale);
            xs[i] = RandomStringUtils.random(scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (String x : xs) {
            sketch.add(x, 1);
        }

        Map<String, Long> actualFreq = new HashMap<String, Long>();
        for (String x : xs) {
            Long val = actualFreq.get(x);
            if (val == null) {
                actualFreq.put(x, 1L);
            } else {
                actualFreq.put(x, val + 1L);
            }
        }

        sketch = CountMinSketch.deserialize(CountMinSketch.serialize(sketch));

        int numErrors = 0;
        for (Map.Entry<String,Long> entry : actualFreq.entrySet()) {
            String key = entry.getKey();
            long count = entry.getValue();
            double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }
        for (int i = 0; i < absentItems; i++) {
            int scale = r.nextInt(maxScale);
            String key = RandomStringUtils.random(scale);
            Long value = actualFreq.get(key);
            long count = (value == null) ? 0L : value;
            double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
            if (ratio > epsOfTotalCount) {
                numErrors++;
            }
        }

        double pCorrect = 1.0 - ((double) numErrors) / (numItems + absentItems);
        System.out.println(pCorrect);
        assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);

        assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
    }


    @Test
    public void merge() throws CMSMergeException {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;

        int maxScale = 20;
        Random r = new Random();
        TreeSet<Integer> vals = new TreeSet<Integer>();

        CountMinSketch baseline = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch[] sketchs = new CountMinSketch[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketchs[i] = new CountMinSketch(epsOfTotalCount, confidence, seed);
            for (int j = 0; j < cardinality; j++) {
                int scale = r.nextInt(maxScale);
                int val = r.nextInt(1 << scale);
                vals.add(val);
                sketchs[i].add(val, 1);
                baseline.add(val, 1);
            }
        }

        CountMinSketch merged = CountMinSketch.merge(sketchs);

        assertEquals(baseline.size(), merged.size());
        assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
        assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
        for (int val : vals) {
            assertEquals(baseline.estimateCount(val), merged.estimateCount(val));
        }
    }

    @Test
    public void testMergeEmpty() throws CMSMergeException {
        assertNull(CountMinSketch.merge());
    }

    @Test(expected = CMSMergeException.class)
    public void testUncompatibleMerge() throws CMSMergeException {
        CountMinSketch cms1 = new CountMinSketch(1, 1, 0);
        CountMinSketch cms2 = new CountMinSketch(0.1, 0.1, 0);
        CountMinSketch.merge(cms1, cms2);
    }
}
