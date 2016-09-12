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

        Fields valueField = new Fields( "TransAmt" ).applyTypes(Integer.class);
        valueField.setComparator("TransAmt",Collections.reverseOrder());

        Pipe pipe3ForSink=new Pipe("temp",Input_Pipe);

        pipe3ForSink= new GroupBy( Input_Pipe, new Fields("AccNo"), valueField);
        pipe3ForSink=new Each(pipe3ForSink,Fields.ALL, new CountFunction(), Fields.ALL);

        Pipe pipe4Sink=new Pipe("temp2",pipe3ForSink);
        Pipe pipeForSink2=new Each(pipe3ForSink,new Fields(0,4),new TwoFilter());

        return  pipeForSink2;

    }

    public static void main(String[] args) {

        // Input File details
        String transactionFile="C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/InputFiles/Transactions_CSV.txt";
        Fields ifields=new Fields("AccNo","TransTyp","TransAmt","TransDate").applyTypes(Integer.class,String.class,Integer.class,String.class);
        Tap sourceTap=new FileTap(new TextDelimited(ifields, true, ","), transactionFile);
        Pipe sourcePipe= new Pipe("pipe for source");

        // Output file 1 details (Less than 500 balance)
        String outputFile2="C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/OutputFiles/salary2ndHighest_OUT.txt";
        Tap sink2=new FileTap(new TextDelimited(true, ","), outputFile2);
        Pipe pipeForSink2=new Pipe("another pipe",sourcePipe);

        CascAssn6_2ndHighestSalary CascAssn6_2ndHighestSalary_Obj = new CascAssn6_2ndHighestSalary();

        Pipe salary2ndHighest_OUT =CascAssn6_2ndHighestSalary_Obj.salary2ndHighest(sourcePipe);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(sourcePipe, sourceTap)
                .addTailSink(salary2ndHighest_OUT, sink2);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();


        System.out.println(" ");
        System.out.println(" ");
        System.out.println("************* DONE ***************");
        System.out.println(" ");
    }

}

class CountFunction extends BaseOperation implements Function {
    Object previous=0;
    int cnt=1;
    public CountFunction() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry arguments = functionCall.getArguments();

        if(arguments.getObject("AccNo").equals(previous))
            cnt=cnt+1;
        else cnt=1;
        previous=arguments.getObject("AccNo");

        // create a Tuple to hold our result values
        Tuple result = new Tuple();
        result.add(cnt);

        // insert some values into the result Tuple

        // return the result Tuple
        functionCall.getOutputCollector().add( result );
    }
}

class TwoFilter extends BaseOperation implements Filter {
    Object previous=0;

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry argument=filterCall.getArguments();


        if(argument.getObject(0).equals(previous) && argument.getObject(1).toString().equals("2"))
        {
            return false;
        }

        else
        {previous=argument.getObject(0);
            return true;}
    }

}

