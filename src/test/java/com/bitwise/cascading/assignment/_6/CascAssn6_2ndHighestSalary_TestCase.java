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
    CascAssn6_2ndHighestSalary CascAssn6_2ndHighestSalary_Obj = new CascAssn6_2ndHighestSalary();

    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("AccNo","TransTyp","TransAmt","TransDate").applyTypes(Integer.class,String.class,Integer.class,String.class))
                .addTuple("1001","CP","6000","10/10/1982")
                .addTuple("1001","CNP","4000","10/10/1982")
                .addTuple("1001","CP","10000","10/10/1982")
                .addTuple("1001","CP","8000","10/10/1982")
                .build();
    }

    @Test
    public void salary2ndHighestTest() {
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn6_2ndHighestSalary_Obj.salary2ndHighest(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("AccNo","TransTyp","TransAmt","TransDate","cnt"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),1);

        assertEquals(actual.get(0).getString(0),"1001");
        assertEquals(actual.get(0).getString(1),"CP");
        assertEquals(actual.get(0).getString(2),"8000");
        assertEquals(actual.get(0).getString(3),"10/10/1982");
        assertEquals(actual.get(0).getString(4),"2");
    }

}
