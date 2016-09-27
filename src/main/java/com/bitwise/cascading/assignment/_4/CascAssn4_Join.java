package com.bitwise.cascading.assignment._4;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
public class CascAssn4_Join {
    public Pipe joinFiles(Pipe  customer_data_Pipe, Pipe transaction_data_Pipe){

        Pipe  HashJoin_data_Pipe = new CoGroup(customer_data_Pipe,new Fields("Account_Number"),transaction_data_Pipe,new Fields("Account_Number1"),new InnerJoin());
        return HashJoin_data_Pipe;
    }
    public static void main(String[] args) {



    }
}
