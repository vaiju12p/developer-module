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
    CascAssn2_FilterRecords CascAssn2_FilterRecords_Obj = new CascAssn2_FilterRecords();

    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("AccountNumber", "Transaction_Type", "Transaction_Amount","Transaction_Date"))
                .addTuple("1003","CP","4000","12/02/2016")
                .addTuple("1004","CP","5000","12/12/2016")
                .addTuple("1003","CNP","8000","12/12/2016")
                .addTuple("1004","CNP","3000","12/21/2016 ")
                .build();
    }

    @Test
    public void extractMonthTest() {
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn2_FilterRecords_Obj.filterTranTypeCNP(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("AccountNumber", "Transaction_Type", "Transaction_Amount","Transaction_Date"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),2);

        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"CP");
        assertEquals(actual.get(0).getString(2),"4000");
        assertEquals(actual.get(0).getString(3),"12/02/2016");
    }



}