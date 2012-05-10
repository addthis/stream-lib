package com.clearspring.analytics.stream.quantile;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class QDigestTest
{
    @Test
    public void testOnSquares()
    {
        double compressionFactor = 1000;
        QDigest digest = new QDigest(compressionFactor);
        int max = 100000;
        for (int j = 0; j < 10; ++j)
        {
            for (long i = 0; i < max; ++i)
            {
                digest.offer(i * i);
            }
        }

        digest = QDigest.deserialize(QDigest.serialize(digest));

        int logCapacity = 1;
        for (long scale = 1; scale < max; scale *= 2, logCapacity++)
        {
            ;
        }
        double eps = logCapacity / compressionFactor;

        for (double i = 0; i < 1; i += 0.1)
        {
            double answer = digest.getQuantile(i);
            System.out.println(i + " => " + answer + " -- " + (i * max) * (i * max));
            double actualRank = Math.min(1.0, Math.sqrt(answer) / max);
            assertTrue(actualRank + " not in " + (i - eps) + " .. " + (i + eps),
                    actualRank > i - eps && actualRank < i + eps);
        }
    }
}
