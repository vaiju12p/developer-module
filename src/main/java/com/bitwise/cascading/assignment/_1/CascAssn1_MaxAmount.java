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
        return transactions_Input_Pipe;
    }

    public static void main(String[] args) throws IOException {



    }

}