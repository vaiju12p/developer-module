package com.bitwise.cascading.assignment._2;

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
public class CascAssn2_FilterRecords_TestCase {
    @Test
    public void itShouldReturnTheFilteredData() {
        Plunger plunger = new Plunger();
        Data corpus = new com.hotels.plunger.DataBuilder(new Fields("AccountNumber", "Transaction_Type", "Transaction_Amount","Transaction_Date"))
                .addTuple(400166,"SAVING",9,10)
                .addTuple(400166,"CNP",85,50)
                .addTuple(400169,"CNP",96,30)
                .addTuple(400169,"CURRENT",87,300)
                .build();
        Pipe inPipe = plunger.newNamedPipe("InPipe", corpus);

        CascAssn2_FilterRecords cascAssn2_filterRecords = new CascAssn2_FilterRecords();

        Bucket bucket = plunger.newBucket(new Fields("AccountNumber", "Transaction_Type", "Transaction_Amount","Transaction_Date"), cascAssn2_filterRecords.filterTranTypeCNP(inPipe));

        List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
        assertEquals(tupleEntries.get(0).getString("AccountNumber"), "400166");

    }
 }