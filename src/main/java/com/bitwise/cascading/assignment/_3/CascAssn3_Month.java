package com.bitwise.cascading.assignment._3;


import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;

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
        Pipe iPipe = new Pipe("iPipe", inp_Pipe);
        iPipe = new Each(iPipe,Fields.ALL,new date_to_month(new Fields("Date_Of_Birth"),new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance","month") ));
        return iPipe;
    }

    public static void main(String[] args) {




    }
}

class date_to_month extends BaseOperation implements Function{

    private final Fields filterFields;
    private final Fields outfields;

    public date_to_month(Fields filterFields, Fields outfields) {
        super(outfields);
        this.filterFields=filterFields;
        this.outfields=outfields;
    }

    @Override
    public void operate(FlowProcess arg0, FunctionCall arg1) {
        try {
            TupleEntry arguments = arg1.getArguments();
            Tuple result = new Tuple();

            extractMonth(arguments, result);

            arg1.getOutputCollector().add( result );
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void extractMonth(TupleEntry arguments, Tuple result) throws ParseException {
        DateFormat date= new SimpleDateFormat("yyyyMMdd");

        Date d=date.parse(arguments.getString( filterFields ));
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(d);
        String fields[]=outfields.toString().replaceAll("\\s", "").replace(",", "\t").replace("'", "").split("\t");
        for(String field: fields) {
            if(field.equalsIgnoreCase("MONTH")){
                result.add(new SimpleDateFormat("MMMM").format(calendar.getTime()));
            }
            else{
            result.add(arguments.getString((Comparable)field));
            }
        }
    }

}
