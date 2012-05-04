package com.clearspring.analytics.stream.quantile;

import org.junit.Test;

import java.util.List;

public class QDigestTest {
    @Test
    public void testOnExampleFromPaper() {
        QDigest digest = new QDigest(3);

        digest.offer(1);
        printRanges(digest);
        digest.offer(3);
        printRanges(digest);
        digest.offer(3);
        printRanges(digest);
        digest.offer(3);
        printRanges(digest);
        digest.offer(3);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(4);
        printRanges(digest);
        digest.offer(5);
        printRanges(digest);
        digest.offer(6);
        printRanges(digest);
        digest.offer(7);
        printRanges(digest);
        digest.offer(8);

        printRanges(digest);

        System.out.println(digest.getQuantile(0.25));
        System.out.println(digest.getQuantile(0.5));
        System.out.println(digest.getQuantile(0.75));

    }

    @Test
    public void testOnSquares() {
        QDigest digest = new QDigest(1000);
        for(int j = 0; j < 10; ++j) {
            for(long i = 0; i < 100000; ++i) {
                digest.offer(i*i);
            }
        }
        // TODO actually test accuracy.
        // Check: actual rank of item returned by getQuantile(i) is i*(1-eps) .. i*(1+eps),
        // where eps = logCapacity/compressionFactor.
        for(double i = 0; i < 1; i += 0.1)
            System.out.println(i + " => " + digest.getQuantile(i) + " -- " + (i*10000L)*(i*10000L));
    }

    private void printRanges(QDigest digest) {
        List<long[]> ranges = digest.toAscRanges();
        StringBuilder sb = new StringBuilder();
        for(long[] range : ranges) {
            if(sb.length() > 0) sb.append("; ");
            sb.append(range[0] + ".." + range[1] + " ==> " + range[2]);
        }
        System.out.println(sb.toString());
    }
}
