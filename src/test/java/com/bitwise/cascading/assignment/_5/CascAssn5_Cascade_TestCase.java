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

    CascAssn5_Cascade CascAssn5_Cascade_Obj = new CascAssn5_Cascade();

    Plunger plunger = new Plunger();

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
        tran_details_CSV_Data = new DataBuilder(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"))
                .addTuple("1001","Mahesh K Ranu","10/10/1982","1234567891","Pune","700")
                .addTuple("1002","Mahesh K Ranu","10/10/1982","1234567891","Pune","550")
                .addTuple("1003","Mahesh K Ranu","10/10/1982","1234567891","Pune","400")
                .addTuple("1004","Mahesh K Ranu","10/10/1982","1234567891","Pune","300")
                .addTuple("1005","Mahesh K Ranu","10/10/1982","1234567891","Pune","800")
                .build();
    }

    @Test
    public void acctountBalanceLess500Test() {
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn5_Cascade_Obj.acctountBalanceLess500(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),2);

        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"Mahesh K Ranu");
        assertEquals(actual.get(0).getString(2),"10/10/1982");
        assertEquals(actual.get(0).getString(3),"1234567891");
        assertEquals(actual.get(0).getString(4),"Pune");
        assertEquals(actual.get(0).getString(5),"400");
    }

    @Test
    public void acctountBalanceMore500Test() {
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn5_Cascade_Obj.acctountBalanceMore500(tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),3);

        assertEquals(actual.get(0).getString(0),"1001");
        assertEquals(actual.get(0).getString(1),"Mahesh K Ranu");
        assertEquals(actual.get(0).getString(2),"10/10/1982");
        assertEquals(actual.get(0).getString(3),"1234567891");
        assertEquals(actual.get(0).getString(4),"Pune");
        assertEquals(actual.get(0).getString(5),"700");
    }
}