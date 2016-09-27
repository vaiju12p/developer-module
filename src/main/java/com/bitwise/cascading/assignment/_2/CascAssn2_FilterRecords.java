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
        Pipe CSV_Input_Pipe = new Pipe("inputPipe", transactions_Input_Pipe);
        CSV_Input_Pipe = new Each(CSV_Input_Pipe, Fields.ALL, new filterToCompareTranAmtNChargOffAmt(new Fields("Transaction_Type"),".*CNP.*"));
        return CSV_Input_Pipe;
    }

    public static void main(String[] args) throws IOException {


    }
}

class filterToCompareTranAmtNChargOffAmt extends BaseOperation implements Filter {

    private final Fields onFilterFields;
    private String regex;
    public filterToCompareTranAmtNChargOffAmt(Fields onFilterFields,String regex) {
        this.onFilterFields=onFilterFields;
        this.regex=regex;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        TupleEntry arguments = filterCall.getArguments();
        return (arguments.getString((Comparable)onFilterFields.toString().replace("'", "")).matches(regex));
    }
}
