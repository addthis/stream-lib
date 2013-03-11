package com.clearspring.analytics.stream.quantile;

import cern.jet.random.Normal;
import cern.jet.random.engine.MersenneTwister64;
import cern.jet.random.engine.RandomEngine;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class QDigestTest
{
    @Test
    public void testComprehensiveOnMixture() {
        RandomEngine r = new MersenneTwister64(0);
        Normal[] dists = new Normal[] {
            new Normal(100, 50, r),
            new Normal(150, 20, r),
            new Normal(500, 300, r),
            new Normal(10000, 10000, r),
            new Normal(1200, 300, r),
        };
        for (int numSamples : new int[] {1, 10, 100, 1000, 10000}) {
            long[][] samples = new long[dists.length][];
            for(int i = 0; i < dists.length; ++i) {
                samples[i] = new long[numSamples];
                for (int j = 0; j < samples[i].length; ++j) {
                    samples[i][j] = (long)Math.max(0, dists[i].nextDouble());
                }
            }
            double compressionFactor = 1000;
            int logCapacity = 1;
            long max = 0;
            for (long[] s : samples) {
                for (long x : s) max = Math.max(max, x);
            }
            for (double scale = 1; scale < max; scale *= 2, logCapacity++)
            {
                ;
            }
            double eps = logCapacity / compressionFactor;

            QDigest[] digests = new QDigest[dists.length];
            for (int i = 0; i < digests.length; ++i) {
                digests[i] = new QDigest(compressionFactor);
                for (long x : samples[i]) {
                    digests[i].offer(x);
                }
            }

            int numTotal = 0;
            for (int i = 0; i < digests.length; ++i) {
                for (double q = 0; q <= 1; q += 0.01) {
                    long res = digests[i].getQuantile(q);
                    double[] actualRank = actualRankOf(res, samples[i]);
                    assertTrue(
                            actualRank[0] + " .. " + actualRank[1] + " outside error bound for  " + q,
                            q >= actualRank[0] - eps && q <= actualRank[1] + eps);
                }

                // Test the same on the union of all distributions up to i-th
                numTotal += samples[i].length;
                long[] total = new long[numTotal];
                int offset = 0;
                QDigest totalDigest = new QDigest(compressionFactor);
                for (int j = 0; j <= i; ++j) {
                    System.arraycopy(samples[j], 0, total, offset, samples[j].length);
                    offset += samples[j].length;
                    totalDigest = QDigest.unionOf(totalDigest, digests[j]);
                }

                for (double q = 0; q <= 1; q += 0.01) {
                    long res = totalDigest.getQuantile(q);
                    double[] actualRank = actualRankOf(res, total);
                    assertTrue(
                            actualRank[0] + " .. " + actualRank[1] + " outside error bound for  " + q,
                            q >= actualRank[0] - eps && q <= actualRank[1] + eps);
                }
            }
        }
    }

    private double[] actualRankOf(long x, long[] ys) {
        int numSmaller = 0;
        int numEqual = 0;
        for (long y : ys) if (y < x) numSmaller++;
        for (long y : ys) if (y == x) numEqual++;
        return new double[] {
                1.0 * numSmaller / ys.length,
                1.0 * (numSmaller + numEqual) / ys.length
        };
    }
}
