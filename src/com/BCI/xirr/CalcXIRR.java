package com.BCI.xirr;


// Imports
import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import java.time.LocalDate;

// Function Template configuration
@FunctionTemplate(
        name = "calc_xirr_udf",
        scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

// uses Aggregate Function
public class CalcXIRR implements AggrFunction {

    @Param
    NullableVarCharHolder when;
    NullableFloat8Holder amount;

    @Output
    private Float8Holder rate;

    @Workspace
    XIRR.Builder xirr;
    Transaction[] txs;
    IntHolder nonNullCount_transactions;

    // setup() function
    public void setup() {

        System.out.println("STDOUT: Calling setup() in calc_xirr_udf ");
        System.err.println("STDERR: Calling setup() in calc_xirr_udf ");

        // Initialize the working variables
        nonNullCount_transactions = new IntHolder();
        nonNullCount_transactions.value = 0;
        when = new NullableVarCharHolder();
        amount = new NullableFloat8Holder();

        txs = new Transaction[8];
        xirr = new XIRR.Builder();
        rate = new Float8Holder();
        rate.value = 0;

    }

    // add() function
    @Override
    public void add() {

        nonNullCount_transactions.value += 1;

        txs[nonNullCount_transactions.value] = new Transaction(amount.value, when.buffer.toString());

        System.out.println("STDOUT: Calling add() in calc_xirr_udf ");

    }

    // output() function
    @Override
    public void output() {

        System.out.println("STDOUT: Calling output() in calc_xirr_udf ");

        rate.value = new XIRR(txs).xirr();
        System.out.println(rate.value);

    }

    // reset() function
    @Override
    public void reset() {
        nonNullCount_transactions.value = 0;
        rate.value = 0;

        System.out.println("STDOUT: Calling reset() in calc_xirr_udf ");

    }
}

class main {

    public static void main(String[] args) {
        System.out.println("Anyone for an XIRR Calculation?");

        Transaction[] txs;
        double[] amounts;
        LocalDate[] dates;

        int c = 4;
        txs = new Transaction[c];

        txs[0] = new Transaction(-1000, "2016-01-15");
        txs[1] = new Transaction(-2500, "2016-02-08");
        txs[2] = new Transaction(-1000, "2016-04-17");
        txs[3] = new Transaction( 5050, "2016-08-24");

        double rate1 = new XIRR(
                txs[0], txs[1], txs[2], txs[3]
        ).xirr();
        System.out.println(rate1);

        double rate2 = new XIRR(
                           new Transaction(-1000, "2016-01-15"),
                                new Transaction(-2500, "2016-02-08"),
                                new Transaction(-1000, "2016-04-17"),
                                new Transaction( 5050, "2016-08-24")
        ).xirr();
        System.out.println(rate2);

        double rate3 = new XIRR(
                txs
        ).xirr();
        System.out.println(rate3);

    }
}

