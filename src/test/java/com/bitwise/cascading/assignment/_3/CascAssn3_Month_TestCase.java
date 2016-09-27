package com.bitwise.cascading.assignment._3;

import static org.junit.Assert.*;

import java.util.List;

import cascading.tuple.TupleEntry;
import org.junit.Before;
import org.junit.Test;



import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;

import cascading.pipe.Each;
import cascading.pipe.Pipe;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import org.junit.Test;
public class CascAssn3_Month_TestCase {
    @Test
    public void itShouldReturnaTheMonthNameForDate() {
    Plunger plunger=new Plunger();
    Data corpus = new com.hotels.plunger.DataBuilder(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"))
            .addTuple(12,23,20160505,"sdf",10,30)
            .addTuple(12,23,20170606,"asasdf",10,30)
            .addTuple(12,23,20180707,"asdfsafd",10,30)
            .addTuple(12,23,20190808,"asdfasdfsd",10,30)
            .build();
    Pipe inPipe=plunger.newNamedPipe("InPipe", corpus);
    CascAssn3_Month cascAssn3_month=new CascAssn3_Month();

    Bucket bucket = plunger.newBucket(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance","month") ,cascAssn3_month.extract_month(inPipe));
    List<TupleEntry> tupleEntries = bucket.result().asTupleEntryList();
    assertEquals(tupleEntries.get(0).getString("month"),  "May");
}

    CascAssn3_Month CascAssn3_Month_Obj = new CascAssn3_Month();

    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"))
                .addTuple("1001","Mahesh K Ranu","19821010","1234567891","Pune","55000")
                .build();
    }

    @Test
    public void extractMonthTest() {
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn3_Month_Obj.extract_month(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance","month"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

       assertEquals(actual.size(),1);

        assertEquals(actual.get(0).getString(0),"1001");
        assertEquals(actual.get(0).getString(1),"Mahesh K Ranu");
        assertEquals(actual.get(0).getString(2),"19821010");
        assertEquals(actual.get(0).getString(3),"1234567891");
        assertEquals(actual.get(0).getString(4),"Pune");
        assertEquals(actual.get(0).getString(5),"55000");
        assertEquals(actual.get(0).getString(6),"October");
    }
}
