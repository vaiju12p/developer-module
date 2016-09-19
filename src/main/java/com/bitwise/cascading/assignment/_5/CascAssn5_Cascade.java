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



        return Input_Pipe;

    }

    public Pipe acctountBalanceMore500(Pipe Input_Pipe){



        return Input_Pipe;

    }


    public static void main(String[] args) {


    }
}

class balanceAmountCompare extends BaseOperation implements Filter
{


    public boolean isRemove( FlowProcess flowProcess, FilterCall call )
    {

        return true;
    }


}