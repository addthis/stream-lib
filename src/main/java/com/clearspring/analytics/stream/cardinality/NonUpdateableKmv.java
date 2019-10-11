package com.clearspring.analytics.stream.cardinality;

import com.yahoo.sketches.theta.CompactSketch;

import java.io.IOException;

public class NonUpdateableKmv extends Kmv {
    public NonUpdateableKmv(CompactSketch sketch){
        this.sketch = sketch;
    }

    @Override
    public boolean offer(Object o) {
        return false;
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        return false;
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        return false;
    }

    @Override
    public int hashCode(){
        return sketch.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof NonUpdateableKmv){
            return this.sketch.equals(((NonUpdateableKmv)o).getSketch());
        } else return false;
    }

}
