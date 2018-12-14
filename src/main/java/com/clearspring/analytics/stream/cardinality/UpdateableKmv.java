package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.yahoo.sketches.theta.UpdateReturnState;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

public class UpdateableKmv extends Kmv implements ICardinality {

    public UpdateableKmv(){
        this.sketch = new UpdateSketchBuilder().setNominalEntries(4096).build();
    }
    public UpdateableKmv(int nominalEntries){
        this.sketch = new UpdateSketchBuilder().setNominalEntries(nominalEntries).build();
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
}
