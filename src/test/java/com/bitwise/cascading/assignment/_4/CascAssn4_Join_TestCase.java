package com.bitwise.cascading.assignment._4;

import static org.junit.Assert.*;

import cascading.tuple.TupleEntry;
import org.junit.Test;

import java.util.List;
import org.junit.Before;



import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CascAssn4_Join_TestCase {
    @Test
    public void itShouldReturnJoinedData() {
        Plunger plunger=new Plunger();
        Data corpus1 = new com.hotels.plunger.DataBuilder(new Fields("Account_Number","Name"))
                .addTuple(20160505,"Vaijnath")
                .addTuple(20170606,"Avinash")
                .addTuple(20180707,"Vijay")
                .addTuple(20190808,"Vikas")
                .build();

        Data corpus2 = new com.hotels.plunger.DataBuilder(new Fields("Account_Number1","Transaction_Amount"))
                .addTuple(20160505,1000)
                .addTuple(20170606,2000)
                .addTuple(20180707,3000)
                .addTuple(20190808,4000)
                .build();
        Pipe inPipe1=plunger.newNamedPipe("InPipe1", corpus1);
        Pipe inPipe2=plunger.newNamedPipe("InPipe2", corpus2);

        CascAssn4_Join cascAssn4_join=new CascAssn4_Join();

        Bucket bucket = plunger.newBucket(new Fields("Account_Number1","Name","Account_Number","Transaction_Amount"),cascAssn4_join.joinFiles(inPipe1,inPipe2));

        List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
        assertEquals(tupleEntries.get(0).getString("Name"),  "Vaijnath");

    }

}
