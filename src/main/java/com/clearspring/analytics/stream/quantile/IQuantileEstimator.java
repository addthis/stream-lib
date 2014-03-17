package com.clearspring.analytics.stream.quantile;

public interface IQuantileEstimator {

    void offer(long value);

    long getQuantile(double q);
}
