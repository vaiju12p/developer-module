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

        Pipe iPipe = new Pipe("iPipe", inp_Pipe);
        iPipe = new Each(iPipe,Fields.ALL,new date_to_month(),Fields.RESULTS);

        return iPipe;
    }

    public static void main(String[] args) {



        // Input File details
        String iFile = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/InputFiles/Assn_7_master.txt";

        Fields iFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance");
        Tap iTap = new FileTap(new TextDelimited(iFields,true,","),iFile);
        Pipe iPipe = new Pipe("iPipe");


        // Output file
        String oFile1 = "C:/Users/tusharvi/Downloads/Hadoop/Notes/Cascading/OutputFiles/CascAssn13_1.txt";
        Fields oFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance","month");
        Tap oTap1 = new FileTap(new TextDelimited(oFields,true,","),oFile1);
        CascAssn3_Month CascAssn3_Month_Obj = new CascAssn3_Month();

        Pipe oPipe_13_1 = CascAssn3_Month_Obj.extract_month(iPipe);

        FlowDef flowDef = FlowDef.flowDef().addSource(iPipe, iTap).addTailSink(oPipe_13_1, oTap1);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.complete();


        System.out.println(" ");
        System.out.println(" ");
        System.out.println("************* DONE ***************");
        System.out.println(" ");
    }
}

class date_to_month extends BaseOperation implements Function{

    static Fields destFields = new Fields("Account_Number","Name","Date_Of_Birth","Phone_Number","City","Account_Balance","month");

    public date_to_month(){

        super(6,destFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry args1 = functionCall.getArguments();
        Tuple result = new Tuple();

        String dt1 =args1.getString("Date_Of_Birth");
        Calendar cald = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        try {
            cald.setTime(sdf.parse(dt1));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        int month = cald.get(Calendar.MONTH);

        String monthStr = new DateFormatSymbols().getMonths()[month];


        result.add(args1.getLong("Account_Number"));
        result.add(args1.getString("Name"));
        result.add(args1.getString("Date_Of_Birth"));
        result.add(args1.getInteger("Phone_Number"));
        result.add(args1.getString("City"));
        result.add(args1.getString("Account_Balance"));
        result.add(monthStr);


        functionCall.getOutputCollector().add( result );
    }


}
