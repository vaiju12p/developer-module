package com.bitwise.cascading.assignment._1;

import static org.junit.Assert.*;

import java.util.List;

import cascading.tuple.TupleEntry;
import org.junit.Before;
import org.junit.Test;

//import com.bitwise.cascading.assignment._1.CascAssn1_MaxAmount;

import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
public class CascAssn1_MaxAmount_TestCase {
    @Test
    public void itShouldReturnMaxAmount() {
        Plunger plunger=new Plunger();
        Data corpus = new com.hotels.plunger.DataBuilder(new Fields("Account_Number","Transaction_Type","Transaction_Amount","Transaction_Date"))
                .addTuple(400166,"vaijnath",9,10)
                .addTuple(400166,"vijay",85,50)
                .addTuple(400169,"vikas",96,30)
                .addTuple(400169,"vikram",87,300)
                .build();
        Pipe inPipe=plunger.newNamedPipe("InPipe", corpus);

        CascAssn1_MaxAmount cascAssn1_maxAmount=new CascAssn1_MaxAmount();
        Bucket bucket = plunger.newBucket(new Fields("Account_Number","max"),cascAssn1_maxAmount.tranWithMaxAmount(inPipe));

        List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
        assertEquals(tupleEntries.get(1).getString("max"),  "96");
    }

}
