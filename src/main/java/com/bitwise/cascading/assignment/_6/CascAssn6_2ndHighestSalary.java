package com.bitwise.cascading.assignment._6;


import java.util.Collections;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CascAssn6_2ndHighestSalary {
    public Pipe salary2ndHighest(Pipe Input_Pipe){

        return  Input_Pipe;

    }

    public static void main(String[] args) {


    }

}

class CountFunction extends BaseOperation implements Function {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {


    }
}

class TwoFilter extends BaseOperation implements Filter {

    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        return true;
    }
}
