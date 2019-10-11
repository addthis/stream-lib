package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.IBuilder;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class UpdateableKmv extends Kmv implements ICardinality {

    public UpdateableKmv(){
        this.sketch = new UpdateSketchBuilder().setNominalEntries(4096).build();
    }
    public UpdateableKmv(int nominalEntries){
        this.sketch = new UpdateSketchBuilder().setNominalEntries(nominalEntries).build();
    }

    public UpdateableKmv(UpdateSketch sketch){
        this.sketch = sketch;
    }


    @Override
    public boolean offer(Object o) {
        final int x = MurmurHash.hash(o);
        return offerHashed(x);
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        UpdateReturnState updateState = ((UpdateSketch)sketch).update(hashedLong);
        return wasUpdated(updateState);
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        UpdateReturnState updateState = ((UpdateSketch)sketch).update(hashedInt);
        return wasUpdated(updateState);
    }

    private boolean wasUpdated(UpdateReturnState state){
        boolean updated = false;
        switch (state){
            case InsertedCountIncremented:
                updated =  true;
                break;
            case InsertedCountNotIncremented:
                updated = true;
                break;
            default:
                updated = false;
        }
        return updated;
    }

    @Override
    public int hashCode(){
        return sketch.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof UpdateableKmv){
            return this.sketch.equals(((UpdateableKmv)o).getSketch());
        } else return false;
    }

    public static class Builder implements IBuilder<ICardinality>, Serializable {
        private final int n;

        public Builder(int n) {
            this.n = n;
        }
        public Builder(){ this.n = 4096;}

        @Override
        public UpdateableKmv build() {
            return new UpdateableKmv(n);
        }

        @Override
        public int sizeof() {
            return n;
        }

        public static UpdateableKmv build(byte[] bytes) throws IOException {
            Sketch sketch = Sketches.wrapSketch(Memory.wrap(bytes));
            try {
                return new UpdateableKmv(((UpdateSketch)sketch));
            }catch(Exception ex){
                throw new IOException("unable to build UpdateSketch from bytes");
            }
        }
    }
}
