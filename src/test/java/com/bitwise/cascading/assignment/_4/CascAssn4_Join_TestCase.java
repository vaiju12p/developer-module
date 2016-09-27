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


    CascAssn4_Join CascAssn4_Join_Obj = new CascAssn4_Join();

    Plunger plunger = new Plunger();

    Data cust_details_CSV_Data = null;
    Pipe cust_details_Pipe_CSV = null;

    Data tran_details_CSV_Data = null;
    Pipe tran_details_Pipe_CSV = null;

    @Before
    public void runFirst(){
        cust_details_CSV_Data = new DataBuilder(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number"))
                .addTuple("1001","Mahesh","10/10/1982","1234567891")
                .addTuple("1002","Rajesh","10/10/1982","1234567891")
                .addTuple("1003","Suresh","10/10/1982","1234567891")
                .addTuple("1004","Kuresh","10/10/1982","1234567891")
                .addTuple("1005","Jayesh","10/10/1982","1234567891")
                .build();

        tran_details_CSV_Data = new DataBuilder(new Fields("Account_Number1","Transaction_Type","Transaction_Amount","Transaction_Date"))
                .addTuple("1003","CP","10000","10/10/1982")
                .addTuple("1004","CP","5000","10/10/1982")
                .addTuple("1003","CNP","8000","10/10/1982")
                .addTuple("1004","CNP","3000","10/10/1982")
                .build();
    }

    @Test
    public void joinFilesTest() {
        cust_details_Pipe_CSV = plunger.newNamedPipe("cust_details_Pipe_CSV", cust_details_CSV_Data);
        tran_details_Pipe_CSV = plunger.newNamedPipe("tran_details_Pipe_CSV", tran_details_CSV_Data);
        Pipe OUT_tran_details_Pipe_CSV = CascAssn4_Join_Obj.joinFiles(cust_details_Pipe_CSV,tran_details_Pipe_CSV);

        Bucket bucket = plunger.newBucket(new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","Account_Number1","Transaction_Type","Transaction_Amount","Transaction_Date"), OUT_tran_details_Pipe_CSV);
        List<Tuple> actual = bucket.result().asTupleList();

        assertEquals(actual.size(),4);

        assertEquals(actual.get(0).getString(0),"1003");
        assertEquals(actual.get(0).getString(1),"Suresh");
        assertEquals(actual.get(0).getString(2),"10/10/1982");
        assertEquals(actual.get(0).getString(3),"1234567891");
        assertEquals(actual.get(0).getString(4),"1003");
        assertEquals(actual.get(0).getString(5),"CP");
        assertEquals(actual.get(0).getString(6),"10000");
        assertEquals(actual.get(0).getString(7),"10/10/1982");
    }

}
