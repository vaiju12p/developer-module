package com.bitwise.cascading.assignment._2;

import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class CascAssn2_FilterRecords {
    public Pipe filterTranTypeCNP(Pipe transactions_Input_Pipe) {

        Pipe CSV_Input_Pipe = new Pipe("Pipe CSV_Input_Pipe", transactions_Input_Pipe);
        CSV_Input_Pipe = new Each(CSV_Input_Pipe, Fields.ALL, new filterToCompareTranAmtNChargOffAmt());

        return CSV_Input_Pipe;
    }

    public static void main(String[] args) throws IOException {

        // Input transaction Details
        String transaction_details = "C:\\Users\\tusharvi\\Downloads\\Hadoop\\Notes\\Cascading\\InputFiles\\Transactions_CSV.txt";
        Fields transaction_Details_Clmns = new Fields("AccountNumber", "Transaction_Type", "Transaction_Amount","Transaction_Date").applyTypes(Integer.class, String.class, Integer.class, String.class);
        Tap transaction_details_CSV_Tap = new FileTap(new TextDelimited(transaction_Details_Clmns, true, ","),
                transaction_details);
        Pipe transactions_Input_Pipe = new Pipe("transactions_Input_Pipe");

        // Output transaction Details - TSV
        String transaction_details_TSV = "C:\\Users\\tusharvi\\Downloads\\Hadoop\\Notes\\Cascading\\OutputFiles\\transaction_details_CP.txt";
        Tap transaction_details_TSV_Tap = new FileTap(new TextDelimited(transaction_Details_Clmns, true, ","),
                transaction_details_TSV, SinkMode.KEEP);
        CascAssn2_FilterRecords CascAssn2_FilterRecords_Obj = new CascAssn2_FilterRecords();
        Pipe transactions_Pipe_TSV_OUT = CascAssn2_FilterRecords_Obj.filterTranTypeCNP(transactions_Input_Pipe);

        FlowDef flowDef = FlowDef.flowDef().addSource(transactions_Input_Pipe, transaction_details_CSV_Tap)
                .addTailSink(transactions_Pipe_TSV_OUT, transaction_details_TSV_Tap);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();

        System.out.println("*************************** DONE ****************************");

    }
}

class filterToCompareTranAmtNChargOffAmt extends BaseOperation implements Filter {

    public filterToCompareTranAmtNChargOffAmt() {
        super(4, Fields.ALL);
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        TupleEntry args = filterCall.getArguments();
        String Transaction_Type = args.getString("Transaction_Type");

        return (Transaction_Type.equals("CNP"));
    }
}
