package com.clearspring.analytics.stream.frequency;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import com.clearspring.analytics.stream.membership.Filter;
import com.clearspring.analytics.util.Preconditions;

public class ShortCountMinSketch implements IFrequency, Serializable {

   public static final long PRIME_MODULUS = (1L << 31) - 1;
   private static final long serialVersionUID = -5084982213094657923L;

   int depth;
   int width;
   short[][] table;
   long[] hashA;
   long size;
   double eps;
   double confidence;

   ShortCountMinSketch() {
   }

   public ShortCountMinSketch(int depth, int width, int seed) {
      this.depth = depth;
      this.width = width;
      this.eps = 2.0 / width;
      this.confidence = 1 - 1 / Math.pow(2, depth);
      initTablesWith(depth, width, seed);
   }

   public ShortCountMinSketch(double epsOfTotalCount, double confidence, int seed) {
      // 2/w = eps ; w = 2/eps
      // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
      this.eps = epsOfTotalCount;
      this.confidence = confidence;
      this.width = (int) Math.ceil(2 / epsOfTotalCount);
      this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
      initTablesWith(depth, width, seed);
   }

   ShortCountMinSketch(int depth, int width, long size, long[] hashA, short[][] table) {
      this.depth = depth;
      this.width = width;
      this.eps = 2.0 / width;
      this.confidence = 1 - 1 / Math.pow(2, depth);
      this.hashA = hashA;
      this.table = table;

      Preconditions.checkState(size >= 0, "The size cannot be smaller than ZER0: " + size);
      this.size = size;
   }

   @Override
   public String toString() {
      return "ShortCountMinSketch{" +
            "eps=" + eps +
            ", confidence=" + confidence +
            ", depth=" + depth +
            ", width=" + width +
            ", size=" + size +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      final ShortCountMinSketch that = (ShortCountMinSketch) o;

      if (depth != that.depth) {
         return false;
      }
      if (width != that.width) {
         return false;
      }

      if (Double.compare(that.eps, eps) != 0) {
         return false;
      }
      if (Double.compare(that.confidence, confidence) != 0) {
         return false;
      }

      if (size != that.size) {
         return false;
      }

      if (!Arrays.deepEquals(table, that.table)) {
         return false;
      }
      return Arrays.equals(hashA, that.hashA);
   }

   @Override
   public int hashCode() {
      int result;
      long temp;
      result = depth;
      result = 31 * result + width;
      result = 31 * result + Arrays.deepHashCode(table);
      result = 31 * result + Arrays.hashCode(hashA);
      result = 31 * result + (int) (size ^ (size >>> 32));
      temp = Double.doubleToLongBits(eps);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(confidence);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
   }

   private void initTablesWith(int depth, int width, int seed) {
      this.table = new short[depth][width];
      this.hashA = new long[depth];
      Random r = new Random(seed);
      // We're using a linear hash functions
      // of the form (a*x+b) mod p.
      // a,b are chosen independently for each hash function.
      // However we can set b = 0 as all it does is shift the results
      // without compromising their uniformity or independence with
      // the other hashes.
      for (int i = 0; i < depth; ++i) {
         hashA[i] = r.nextInt(Integer.MAX_VALUE);
      }
   }

   public double getRelativeError() {
      return eps;
   }

   public double getConfidence() {
      return confidence;
   }

   int hash(long item, int i) {
      long hash = hashA[i] * item;
      // A super fast way of computing x mod 2^p-1
      // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
      // page 149, right after Proposition 7.
      hash += hash >> 32;
      hash &= PRIME_MODULUS;
      // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
      return ((int) hash) % width;
   }

   private static void checkSizeAfterOperation(long previousSize, String operation, long newSize) {
      if (newSize < previousSize) {
         throw new IllegalStateException("Overflow error: the size after calling `" + operation +
               "` is smaller than the previous size. " +
               "Previous size: " + previousSize +
               ", New size: " + newSize);
      }
   }

   private void checkSizeAfterAdd(String item, long count) {
      long previousSize = size;
      size += count;
      checkSizeAfterOperation(previousSize, "add(" + item + "," + count + ")", size);
   }

   public void addOne(long item) {
      add(item, (short) 1);

      checkSizeAfterAdd(String.valueOf(item), (short) 1);
   }

   @Override
   public void add(long item, long count) {
      if (count < 0) {
         // Actually for negative increments we'll need to use the median
         // instead of minimum, and accuracy will suffer somewhat.
         // Probably makes sense to add an "allow negative increments"
         // parameter to constructor.
         throw new IllegalArgumentException("Negative increments not implemented");
      }
      if (count > Short.MAX_VALUE) {
         count = Short.MAX_VALUE;
      }

      for (int i = 0; i < depth; ++i) {
         int hash = hash(item, i);
         table[i][hash] += count;
         // check for short overflow
         if (table[i][hash] < 0) {
            table[i][hash] = Short.MAX_VALUE;
         }
      }

      checkSizeAfterAdd(String.valueOf(item), count);
   }

   @Override
   public void add(String item, long count) {
      if (count < 0) {
         // Actually for negative increments we'll need to use the median
         // instead of minimum, and accuracy will suffer somewhat.
         // Probably makes sense to add an "allow negative increments"
         // parameter to constructor.
         throw new IllegalArgumentException("Negative increments not implemented");
      }
      if (count > Short.MAX_VALUE) {
         count = Short.MAX_VALUE;
      }
      int[] buckets = Filter.getHashBuckets(item, depth, width);
      for (int i = 0; i < depth; ++i) {
         int bucket = buckets[i];
         table[i][bucket] += count;
         // check for short overflow
         if (table[i][bucket] < 0) {
            table[i][bucket] = Short.MAX_VALUE;
         }
      }

      checkSizeAfterAdd(item, count);
   }

   @Override
   public long size() {
      return size;
   }

   /**
    * The estimate is correct within 'epsilon' * (total item count), with probability 'confidence'.
    */
   @Override
   public long estimateCount(long item) {
      short res = Short.MAX_VALUE;
      for (int i = 0; i < depth; ++i) {
         // cast is possible due to table stores just short values
         res = (short) Math.min(res, table[i][hash(item, i)]);
      }
      return res;
   }

   @Override
   public long estimateCount(String item) {
      short res = Short.MAX_VALUE;
      int[] buckets = Filter.getHashBuckets(item, depth, width);
      for (int i = 0; i < depth; ++i) {
         // cast is possible due to table stores just short values
         res = (short) Math.min(res, table[i][buckets[i]]);
      }
      return res;
   }

   /**
    * Merges count min sketches to produce a count min sketch for their combined streams
    *
    * @param estimators
    * @return merged estimator or null if no estimators were provided
    * @throws CMSMergeException
    *            if estimators are not mergeable (same depth, width and seed)
    */
   public static ShortCountMinSketch merge(ShortCountMinSketch... estimators) throws CMSMergeException {
      ShortCountMinSketch merged = null;
      if (estimators != null && estimators.length > 0) {
         int depth = estimators[0].depth;
         int width = estimators[0].width;
         long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

         short[][] table = new short[depth][width];
         long size = 0;

         for (ShortCountMinSketch estimator : estimators) {
            if (estimator.depth != depth) {
               throw new CMSMergeException("Cannot merge estimators of different depth");
            }
            if (estimator.width != width) {
               throw new CMSMergeException("Cannot merge estimators of different width");
            }
            if (!Arrays.equals(estimator.hashA, hashA)) {
               throw new CMSMergeException("Cannot merge estimators of different seed");
            }

            for (int i = 0; i < table.length; i++) {
               for (int j = 0; j < table[i].length; j++) {
                  table[i][j] += estimator.table[i][j];
               }
            }

            long previousSize = size;
            size += estimator.size;
            checkSizeAfterOperation(previousSize, "merge(" + estimator + ")", size);
         }

         merged = new ShortCountMinSketch(depth, width, size, hashA, table);
      }

      return merged;
   }

   public static byte[] serialize(ShortCountMinSketch sketch) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(bos);
      try {
         s.writeLong(sketch.size);
         s.writeInt(sketch.depth);
         s.writeInt(sketch.width);
         for (int i = 0; i < sketch.depth; ++i) {
            s.writeLong(sketch.hashA[i]);
            for (int j = 0; j < sketch.width; ++j) {
               s.writeShort(sketch.table[i][j]);
            }
         }
         return bos.toByteArray();
      } catch (IOException e) {
         // Shouldn't happen
         throw new RuntimeException(e);
      }
   }

   public static ShortCountMinSketch deserialize(byte[] data) {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      DataInputStream s = new DataInputStream(bis);
      try {
         ShortCountMinSketch sketch = new ShortCountMinSketch();
         sketch.size = s.readLong();
         sketch.depth = s.readInt();
         sketch.width = s.readInt();
         sketch.eps = 2.0 / sketch.width;
         sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
         sketch.hashA = new long[sketch.depth];
         sketch.table = new short[sketch.depth][sketch.width];
         for (int i = 0; i < sketch.depth; ++i) {
            sketch.hashA[i] = s.readLong();
            for (int j = 0; j < sketch.width; ++j) {
               sketch.table[i][j] = s.readShort();
            }
         }
         return sketch;
      } catch (IOException e) {
         // Shouldn't happen
         throw new RuntimeException(e);
      }
   }

   @SuppressWarnings("serial")
   protected static class CMSMergeException extends FrequencyMergeException {

      public CMSMergeException(String message) {
         super(message);
      }
   }
}
