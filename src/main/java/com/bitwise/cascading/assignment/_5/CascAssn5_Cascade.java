package com.bitwise.cascading.assignment._5;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
public class CascAssn5_Cascade {
    public Pipe acctountBalanceLess500(Pipe Input_Pipe){

        Not not1 = new Not(new balanceAmountCompare());
        Pipe oPipe_less500 = new Each(Input_Pipe,not1);

        return oPipe_less500;

    }

    public Pipe acctountBalanceMore500(Pipe Input_Pipe){

        Pipe oPipe_more500 = new Each(Input_Pipe,new balanceAmountCompare());

        return oPipe_more500;

    }


    public static void main(String[] args) {


        // Input File details
        String iFile = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/InputFiles/Assn_7_master.txt";
        Fields iFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance");
        Tap iTap = new FileTap(new TextDelimited(iFields,true,","),iFile);
        Pipe iPipe = new Pipe("iPipe");
        Pipe iPipe2 = new Pipe("iPipe2");

        // Output file 1 details (Less than 500 balance)
        String oFile1 = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/OutputFiles/CascAssn_less_500.txt";
        Tap oTap1 = new FileTap(new TextDelimited(iFields,true,","),oFile1);
        Pipe oPipe_less500 = new Pipe("oPipe_less500",iPipe);

        // Output file 1 details (More than 500 balance)
        String oFile2 = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/OutputFiles/CascAssn_more_500.txt";
        Tap oTap2 = new FileTap(new TextDelimited(iFields,true,","),oFile2);
        Pipe oPipe_more500 = new Pipe("oPipe_more500",iPipe2);

        CascAssn5_Cascade CascAssn5_Cascade_Obj = new CascAssn5_Cascade();

        Pipe acctountBalanceLessThan500_OUT =CascAssn5_Cascade_Obj.acctountBalanceLess500(iPipe);
        Pipe acctountBalanceMoreThan500_OUT =CascAssn5_Cascade_Obj.acctountBalanceMore500(iPipe2);

        FlowDef flowDef1 = FlowDef.flowDef().setName("fd1").addSource(iPipe, iTap).addTailSink(acctountBalanceLessThan500_OUT, oTap1);
        FlowDef flowDef2 = FlowDef.flowDef().setName("fd2").addSource(iPipe2, iTap).addTailSink(acctountBalanceMoreThan500_OUT, oTap2);

        Flow<?> flow1 = new LocalFlowConnector().connect(flowDef1);
        Flow<?> flow2 = new LocalFlowConnector().connect(flowDef2);


        CascadeConnector  connector = new CascadeConnector();
        Cascade casc = connector.connect(flow1,flow2);
        casc.complete();


        System.out.println(" ");
        System.out.println(" ");
        System.out.println("************* DONE ***************");
        System.out.println(" ");
    }
}

class balanceAmountCompare extends BaseOperation implements Filter
{

    public balanceAmountCompare(){
        super(1,Fields.ALL);
    }

    public boolean isRemove( FlowProcess flowProcess, FilterCall call )
    {
// get the arguments TupleEntry
        TupleEntry arguments = call.getArguments();

        return arguments.getLong("Account_Balance") <= 500;
    }


}