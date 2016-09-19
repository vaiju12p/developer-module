package com.bitwise.cascading.assignment._3;


import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
public class CascAssn3_Month {
    public Pipe extract_month(Pipe inp_Pipe){

        return inp_Pipe;
    }

    public static void main(String[] args) {




    }
}

class date_to_month extends BaseOperation implements Function{


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {


    }


}
