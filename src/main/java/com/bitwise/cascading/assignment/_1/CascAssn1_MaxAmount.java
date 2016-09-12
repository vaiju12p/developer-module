package com.bitwise.cascading.assignment._1;

import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.aggregator.MaxValue;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.GroupBy;


public class CascAssn1_MaxAmount {


    public Pipe tranWithMaxAmount(Pipe transactions_Input_Pipe) {

        Pipe CSV_Input_Pipe = new Pipe("Pipe CSV_Input_Pipe",transactions_Input_Pipe);

        CSV_Input_Pipe = new GroupBy(CSV_Input_Pipe,new Fields("Account_Number"));
        CSV_Input_Pipe = new Every(CSV_Input_Pipe,new Fields("Transaction_Amount"),new MaxValue(),Fields.ALL);

        return CSV_Input_Pipe;
    }

    public static void main(String[] args) throws IOException {

        // Input transaction Details
        String transaction_details = "C:\\Users\\tusharvi\\Downloads\\Hadoop\\Notes\\Cascading\\InputFiles\\Transactions_CSV.txt";
        Fields transaction_Details_Clmns = new Fields("Account_Number","Transaction_Type","Transaction_Amount","Transaction_Date").applyTypes(Integer.class,String.class,Integer.class,String.class);
        Tap transaction_details_CSV_Tap = new FileTap(new TextDelimited(transaction_Details_Clmns, true, ","), transaction_details);
        Pipe transactions_Input_Pipe = new Pipe("transactions_Input_Pipe");

        // Output transaction Details - TSV
        String transaction_details_TSV = "C:\\Users\\tusharvi\\Downloads\\Hadoop\\Notes\\Cascading\\OutputFiles\\transaction_details_TSV.txt";
        Fields transaction_Details_Clmns_OUT = new Fields("Account_Number","max");
        Tap transaction_details_TSV_Tap = new FileTap(new TextDelimited(transaction_Details_Clmns_OUT, false, "\t"),transaction_details_TSV, SinkMode.KEEP);
        CascAssn1_MaxAmount CascAssn1_MaxAmount_Obj = new CascAssn1_MaxAmount();
        Pipe transactions_Pipe_TSV_OUT = CascAssn1_MaxAmount_Obj.tranWithMaxAmount(transactions_Input_Pipe);

        FlowDef flowDef = FlowDef.flowDef().addSource(transactions_Input_Pipe, transaction_details_CSV_Tap).addTailSink(transactions_Pipe_TSV_OUT,
                transaction_details_TSV_Tap);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();

        System.out.println("*************************** DONE ****************************");

    }

}