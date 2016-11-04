package com.clearspring.analytics.stream.frequency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import com.clearspring.analytics.stream.frequency.ShortCountMinSketch.CMSMergeException;

public class ShortCountMinSketchTest {
   /*
    * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
    * with the License. You may obtain a copy of the License at
    *
    * http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
    * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
    * the specific language governing permissions and limitations under the License.
    */

   @Test(expected = IllegalStateException.class)
   public void negativeSize() {
      new ShortCountMinSketch(20, 4, (short) -1, new long[] { 1 }, new short[][] { { 10, 20 }, { 100, 200 } });
   }

   @Test
   public void testSize() throws CMSMergeException {
      ShortCountMinSketch sketch = new ShortCountMinSketch(0.00001, 0.99999, 1);
      assertEquals(0, sketch.size(), 0);

      sketch.add(1, (short) 11);
      sketch.add(2, (short) 22);
      sketch.add(3, (short) 33);

      long expectedSize = 11 + 22 + 33;
      assertEquals(expectedSize, sketch.size());
   }
   
   @Test
   public void testSizeShortOverflowWithMaxShort() throws CMSMergeException {
      double confidence = 0.999;
      double epsilon = 0.0001;
      int seed = 1;

      ShortCountMinSketch sketch = new ShortCountMinSketch(epsilon, confidence, seed);

      long freq1 = Long.MAX_VALUE;
      short freq2 = 156;

      sketch.add(1, freq1);
      sketch.add(2, freq2);

      ShortCountMinSketch newSketch = ShortCountMinSketch.merge(sketch, sketch);

      short maxShortFromOverflow = Short.MAX_VALUE;
      
      long expectedSize = 2 * (maxShortFromOverflow + freq2);
      assertEquals(expectedSize, newSketch.size());
   }

   @Test
   public void testSizeCanStoreShort() throws CMSMergeException {
      double confidence = 0.999;
      double epsilon = 0.0001;
      int seed = 1;

      ShortCountMinSketch sketch = new ShortCountMinSketch(epsilon, confidence, seed);

      short freq1 = Short.MAX_VALUE;
      short freq2 = 156;

      sketch.add(1, freq1);
      sketch.add(2, freq2);

      ShortCountMinSketch newSketch = ShortCountMinSketch.merge(sketch, sketch);

      long expectedSize = 2 * (freq1 + freq2);
      assertEquals(expectedSize, newSketch.size());
   }

   @Test
   public void testAccuracy() {
      int seed = Short.MAX_VALUE / 2;
      Random r = new Random(seed);
      int numItems = 10_000;
      int[] xs = new int[numItems];
      int maxScale = 20;
      for (int i = 0; i < numItems; i++) {
         int scale = r.nextInt(maxScale);
         xs[i] = r.nextInt(1 << scale);
      }

      double epsOfTotalCount = 0.0001;
      double confidence = 0.99;

      ShortCountMinSketch sketch = new ShortCountMinSketch(epsOfTotalCount, confidence, seed);
      for (int x : xs) {
         sketch.add(x, (short) 1);
      }

      int[] actualFreq = new int[1 << maxScale];
      for (int x : xs) {
         actualFreq[x]++;
      }

      sketch = ShortCountMinSketch.deserialize(ShortCountMinSketch.serialize(sketch));

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
      int numItems = 1000000;
      int absentItems = numItems * 10;
      String[] xs = new String[numItems];
      for (int i = 0; i < numItems; i++) {
         xs[i] = UUID.randomUUID().toString();
      }

      double epsOfTotalCount = 0.0001;
      double confidence = 0.99;

      ShortCountMinSketch sketch = new ShortCountMinSketch(epsOfTotalCount, confidence, seed);
      for (String x : xs) {
         sketch.add(x, (short) 1);
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

      sketch = ShortCountMinSketch.deserialize(ShortCountMinSketch.serialize(sketch));

      int numErrors = 0;
      for (Map.Entry<String, Long> entry : actualFreq.entrySet()) {
         String key = entry.getKey();
         long count = entry.getValue();
         double ratio = ((double) (sketch.estimateCount(key) - count)) / numItems;
         if (ratio > epsOfTotalCount) {
            numErrors++;
         }
      }
      for (int i = 0; i < absentItems; i++) {
         String key = UUID.randomUUID().toString();
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
      int numToMerge = 3;
      int cardinality = 10_000;

      double epsOfTotalCount = 0.0001;
      double confidence = 0.99;
      int seed = Short.MAX_VALUE / 2;

      int maxScale = 20;
      Random r = new Random();
      TreeSet<Integer> vals = new TreeSet<Integer>();

      ShortCountMinSketch baseline = new ShortCountMinSketch(epsOfTotalCount, confidence, seed);
      ShortCountMinSketch[] sketchs = new ShortCountMinSketch[numToMerge];
      for (int i = 0; i < numToMerge; i++) {
         sketchs[i] = new ShortCountMinSketch(epsOfTotalCount, confidence, seed);
         for (int j = 0; j < cardinality; j++) {
            int scale = r.nextInt(maxScale);
            int val = r.nextInt(1 << scale);
            vals.add(val);
            sketchs[i].add(val, (short) 1);
            baseline.add(val, (short) 1);
         }
      }

      ShortCountMinSketch merged = ShortCountMinSketch.merge(sketchs);

      assertEquals(baseline.size(), merged.size());
      assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
      assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
      for (int val : vals) {
         assertEquals(baseline.estimateCount(val), merged.estimateCount(val));
      }
   }

   @Test
   public void testMergeEmpty() throws CMSMergeException {
      assertNull(ShortCountMinSketch.merge());
   }

   @Test(expected = CMSMergeException.class)
   public void testUncompatibleMerge() throws CMSMergeException {
      ShortCountMinSketch cms1 = new ShortCountMinSketch(1, 1, 0);
      ShortCountMinSketch cms2 = new ShortCountMinSketch(0.1, 0.1, 0);
      ShortCountMinSketch.merge(cms1, cms2);
   }

   private static void checkCountMinSketchSerialization(ShortCountMinSketch cms)
         throws IOException, ClassNotFoundException {
      byte[] bytes = ShortCountMinSketch.serialize(cms);
      ShortCountMinSketch serializedCms = (ShortCountMinSketch) ShortCountMinSketch.deserialize(bytes);

      assertEquals(cms, serializedCms);
   }

   @Test
   public void testSerializationForDepthCms() throws IOException, ClassNotFoundException {
      checkCountMinSketchSerialization(new ShortCountMinSketch(12, 2045, 1));
   }

   @Test
   @Ignore("depends on the os. But works.")
   public void testSerializationForConfidenceCms() throws IOException, ClassNotFoundException {
      checkCountMinSketchSerialization(new ShortCountMinSketch(0.0001, 0.99999999999, 1));
   }

   @Test
   public void testEquals() {
      double eps1 = 0.0001;
      double eps2 = 0.000001;
      double confidence = 0.99;
      int seed = 1;

      final ShortCountMinSketch sketch1 = new ShortCountMinSketch(eps1, confidence, seed);
      assertEquals(sketch1, sketch1);

      final ShortCountMinSketch sketch2 = new ShortCountMinSketch(eps1, confidence, seed);
      assertEquals(sketch1, sketch2);

      final CountMinSketch sketch3 = new ConservativeAddSketch(eps1, confidence, seed);
      assertNotEquals(sketch1, sketch3);

      assertNotEquals(sketch1, null);

      sketch1.add(1, (short) 123);
      sketch2.add(1, (short) 123);
      assertEquals(sketch1, sketch2);

      sketch1.add(1, (short) 4);
      assertNotEquals(sketch1, sketch2);

      final ShortCountMinSketch sketch4 = new ShortCountMinSketch(eps1, confidence, seed);
      final ShortCountMinSketch sketch5 = new ShortCountMinSketch(eps2, confidence, seed);
      assertNotEquals(sketch4, sketch5);

      sketch3.add(1, (short) 7);
      sketch4.add(1, (short) 7);
      assertNotEquals(sketch4, sketch5);
   }

   @Test
   public void testToString() {
      double eps = 0.0001;
      double confidence = 0.99;
      int seed = 1;

      final ShortCountMinSketch sketch = new ShortCountMinSketch(eps, confidence, seed);
      assertEquals("ShortCountMinSketch{" +
            "eps=" + eps +
            ", confidence=" + confidence +
            ", depth=" + 7 +
            ", width=" + 20000 +
            ", size=" + 0 +
            '}', sketch.toString());

      sketch.add(12, (short) 145);
      assertEquals("ShortCountMinSketch{" +
            "eps=" + eps +
            ", confidence=" + confidence +
            ", depth=" + 7 +
            ", width=" + 20000 +
            ", size=" + 145 +
            '}', sketch.toString());
   }
}
