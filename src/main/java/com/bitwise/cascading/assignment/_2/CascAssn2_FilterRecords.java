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


        return transactions_Input_Pipe;
    }

    public static void main(String[] args) throws IOException {


    }
}

class filterToCompareTranAmtNChargOffAmt extends BaseOperation implements Filter {

    public filterToCompareTranAmtNChargOffAmt() {
      
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {


        return (true);
    }
}
