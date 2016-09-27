package com.bitwise.cascading.assignment._6;


import java.util.Collections;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.*;
import cascading.pipe.Each;
import cascading.pipe.Every;
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
        Pipe tails = new GroupBy(Input_Pipe,Fields.UNKNOWN);
        tails=new Every(tails, new SecondMaxValue(new Fields("TransAmt")));
        return  tails;

    }

    public static void main(String[] args) {


    }

}

class SecondMaxValue  extends BaseOperation<SecondMaxValue.Context>  implements Aggregator<SecondMaxValue.Context>
{
    public static class Context
    {
        long first=0,second=0;
        Tuple t1=new  Tuple();
        Tuple t2=new  Tuple();
    }

    private Fields fields;

    public SecondMaxValue(Fields fields) {
        this.fields=fields;
    }

    public void start( FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall )
    {
        aggregatorCall.setContext( new Context() );
    }

    public void aggregate( FlowProcess flowProcess,
                           AggregatorCall<Context> aggregatorCall )
    {
        TupleEntry arguments = aggregatorCall.getArguments();
        Context context = aggregatorCall.getContext();
        long value=arguments.getInteger((Comparable)fields.toString().replace("'", ""));
        if(context.first<value){
            context.second=context.first;
            context.first=value;
            context.t2=new Tuple(context.t1);
            context.t1=new Tuple(arguments.getTuple());
        }else if(context.second<value){
            context.second=value;
            context.t2=new Tuple(arguments.getTuple());
        }
    }

    public void complete( FlowProcess flowProcess,
                          AggregatorCall<Context> aggregatorCall )
    {
        Context context = aggregatorCall.getContext();
        context.t2.add("2");
        aggregatorCall.getOutputCollector().add(context.t2);	;
    }
}