package com.bitwise.cascading.assignment._5;

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

public class CascAssn5_Cascade_TestCase {
    @Test
    public void itShouldReturnSalaryGreaterThan500() {
        Plunger plunger=new Plunger();
        Data corpus1 = new com.hotels.plunger.DataBuilder(new Fields("Account_Balance","Name"))
                .addTuple(05,"Vaijnath")
                .addTuple(06,"Avinash")
                .addTuple(707,"Vijay")
                .addTuple(808,"Vikas")
                .build();

        Pipe inPipe1=plunger.newNamedPipe("InPipe1", corpus1);


        CascAssn5_Cascade cascAssn5_cascade=new CascAssn5_Cascade();

        Bucket bucket = plunger.newBucket(new Fields("Account_Balance","Name"),cascAssn5_cascade.acctountBalanceLess500(inPipe1));

        List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
        assertEquals(tupleEntries.get(0).getString("Name"),  "Vaijnath");

    }

    @Test
    public void itShouldReturnSalaryLessThan500() {
        Plunger plunger=new Plunger();
        Data corpus1 = new com.hotels.plunger.DataBuilder(new Fields("Account_Balance","Name"))
                .addTuple(05,"Vaijnath")
                .addTuple(06,"Avinash")
                .addTuple(707,"Vijay")
                .addTuple(808,"Vikas")
                .build();


        Pipe inPipe2=plunger.newNamedPipe("InPipe2", corpus1);

        CascAssn5_Cascade cascAssn5_cascade=new CascAssn5_Cascade();

        Bucket bucket1 = plunger.newBucket(new Fields("Account_Balance","Name"),cascAssn5_cascade.acctountBalanceMore500(inPipe2));
        List<TupleEntry> tupleEntries1 = bucket1.result().asTupleEntryList();
        assertEquals(tupleEntries1.get(0).getString("Name"),  "Vijay");
    }


}