package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.util.IBuilder;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;

import java.io.IOException;
import java.io.Serializable;

public abstract class Kmv implements ICardinality {

    protected Sketch sketch;
    public Sketch getSketch(){
        return this.sketch;
    }

    @Override
    public long cardinality() {
        return (long)sketch.getEstimate();
    }

    @Override
    public int sizeof() {
        return sketch.getRetainedEntries();
    }

    @Override
    public byte[] getBytes() throws IOException {
        return sketch.toByteArray();
    }
    
    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        Union union = Union.builder().buildUnion();
        for(ICardinality estimator: estimators){
            if (estimator instanceof Kmv) {
                union.update(((Kmv) estimator).getSketch());
            } else {
                throw new KmvMergeException("Estimators must be of either type UpdateableKmv or NonUpdateableKmv to merge");
            }
        }

        union.update(this.sketch);
        return new NonUpdateableKmv(union.getResult());

    }

    @Override
    public ICardinality intersect(ICardinality... estimators) throws CardinalityMergeException {
        Intersection intersection = Intersection.builder().buildIntersection();
        for(ICardinality estimator: estimators){
            if (estimator instanceof Kmv) {
                intersection.update(((Kmv) estimator).getSketch());
            } else {
                throw new KmvMergeException("Estimators must be of either type UpdateableKmv or NonUpdateableKmv to merge");
            }
        }

        intersection.update(this.sketch);
        return new NonUpdateableKmv(intersection.getResult());
    }



    protected static class KmvMergeException extends CardinalityMergeException {

        public KmvMergeException(String message) {
            super(message);
        }
    }
}
