package com.bitwise.cascading.assignment._6;

import static org.junit.Assert.*;

import java.util.List;

import cascading.tuple.TupleEntry;
import org.junit.Before;
import org.junit.Test;


import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
public class CascAssn6_2ndHighestSalary_TestCase {

    @Test
    public void itShouldReturnTheFilteredData() {
        Plunger plunger=new Plunger();
        Data corpus = new com.hotels.plunger.DataBuilder(new Fields("AccNo","TransTyp","TransAmt","TransDate"))
                .addTuple(400166,"Aurangabad",92,12)
                .addTuple(400166,"Pune",90,211)
                .addTuple(400169,"Mumbai",96,54)
                .addTuple(400169,"Delhi",89,54)
                .build();

        Pipe inPipe=plunger.newNamedPipe("InPipe", corpus);
        CascAssn6_2ndHighestSalary cascAssn6_2ndHighestSalary=new CascAssn6_2ndHighestSalary();

        Bucket bucket = plunger.newBucket(new Fields("AccNo","TransTyp","TransAmt","TransDate","count"),cascAssn6_2ndHighestSalary.salary2ndHighest(inPipe));
        List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
        assertEquals(tupleEntries.get(0).getString("TransTyp"),  "Aurangabad"); assertEquals(tupleEntries.get(0).getString("count"),  "2");
    }


}
