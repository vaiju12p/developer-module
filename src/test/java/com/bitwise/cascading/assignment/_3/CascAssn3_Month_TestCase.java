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
}
