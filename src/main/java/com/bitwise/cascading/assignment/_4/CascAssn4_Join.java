package com.bitwise.cascading.assignment._4;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
public class CascAssn4_Join {
    public Pipe joinFiles(Pipe  customer_data_Pipe, Pipe transaction_data_Pipe){

        Fields joinKey1 = new Fields("Account_Number");
        Fields joinKey2 = new Fields("Account_Number1");
        Fields out_data_Clmns = new Fields("Account_Number1","Name","Date_Of_Birth","Phone_Number","Account_Number","Transaction_Type","Transaction_Amount","Transaction_Date");

        Pipe  	HashJoin_data_Pipe = new CoGroup(customer_data_Pipe,joinKey1,transaction_data_Pipe,joinKey2,out_data_Clmns,new InnerJoin());

        return HashJoin_data_Pipe;
    }
    public static void main(String[] args) {


        // Input Customer Details
        String customer_data= "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/InputFiles/master1.txt";
        Fields customer_data_Clmns = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number");
        Tap customer_data_Tap = new FileTap(new TextDelimited(customer_data_Clmns, true, ","), customer_data );
        Pipe customer_data_Pipe = new Pipe ("customer_data_Pipe");

        String transaction_data= "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/InputFiles/Transactions_CSV.txt";
        Fields transaction_data_Clmns = new Fields("Account_Number1","Transaction_Type","Transaction_Amount","Transaction_Date");
        Tap transaction_data_Tap = new FileTap(new TextDelimited(transaction_data_Clmns, true, ","), transaction_data );
        Pipe transaction_data_Pipe = new Pipe ("transaction_data_Pipe");

        // Output Customer Details - TSV
        String transaction_data_TSV = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/OutputFiles/CascAssn4_OUT.txt";
        Tap transaction_data_TSV_Tap = new FileTap(new TextDelimited(true,","), transaction_data_TSV);


        CascAssn4_Join CascAssn4_Join_Obj = new CascAssn4_Join();
        Pipe HashJoin_data_Pipe = CascAssn4_Join_Obj.joinFiles(customer_data_Pipe,transaction_data_Pipe);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(customer_data_Pipe, customer_data_Tap)
                .addSource(transaction_data_Pipe, transaction_data_Tap)
                .addTailSink(HashJoin_data_Pipe, transaction_data_TSV_Tap);


        FlowConnector fc = new LocalFlowConnector();
        Flow<?> flow= fc.connect(flowDef);
        flow.complete();

        System.out.println(" ");
        System.out.println(" ");
        System.out.println("************* DONE ***************");
        System.out.println(" ");

    }
}
