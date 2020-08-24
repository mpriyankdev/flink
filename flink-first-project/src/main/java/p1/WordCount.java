package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {

    public static void main(String[] args) throws Exception {
        //setup the execution environment , based on where the program is running
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make params available in the web interface
        final ParameterTool params = ParameterTool.fromArgs(args);

        //to make the parameters available globally
        env.getConfig().setGlobalJobParameters(params);

        // read the text file from given input path
        DataSet<String> input = env.readTextFile(params.get("input"));

        //filtering names starting with N
        DataSet<String> filtered = input.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.startsWith("N");
            }
        });

        //return a tuple of (name,1)
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        //groupBy name[at posn 0] and sum the field at posn 1
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);

        //emit result
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            //execute the program
            env.execute("Wordcount Example");

        }


    }


    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<String, Integer>(value, 1);
        }
    }
}


