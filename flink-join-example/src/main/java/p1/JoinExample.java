package p1;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class JoinExample {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
                .map(new MapFunction<String, Tuple2<Integer, String>>()                                     //presonSet = tuple of (1  John)
                {
                    public Tuple2<Integer, String> map(String value) {
                        String[] words = value.split(",");                                                 // words = [ {1} {John}]
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });
        // Read location file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).
                map(new MapFunction<String, Tuple2<Integer, String>>() {                                                                                                 //locationSet = tuple of (1  DC)
                    public Tuple2<Integer, String> map(String value) {
                        String[] words = value.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });

        // join datasets on person_id
        // joined format will be <id, person_name, state>
        //similarly leftOuterJoin and rightOuterJoin can be implemented
        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);         // returns tuple of (1 John DC)
                    }
                });

        // left outer join datasets on person_id
        // joined format will be <id, person_name, state>

        DataSet<Tuple3<Integer, String, String>> leftOuterJoin = personSet.leftOuterJoin(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        // check for nulls
                        if (location == null) {
                            return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
                        }

                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
                    }
                });

        DataSet<Tuple3<Integer, String, String>> rightOuterjoin = personSet.rightOuterJoin(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        // check for nulls
                        if (person == null) {
                            return new Tuple3<Integer, String, String>(location.f0, "NULL", location.f1);
                        }

                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
                    }
                });

        // joined format will be <id, person_name, state>

        DataSet<Tuple3<Integer, String, String>> fullOuterJoin = personSet.fullOuterJoin(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        // check for nulls
                        if (location == null) {
                            return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
                        }
                        // for rightOuterJoin
                        else if (person == null)
                            return new Tuple3<Integer, String, String>(location.f0, "NULL", location.f1);

                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
                    }
                });

        //currently writing only inner-join example
        //enabling the feature of overwrite , can be removed by using some other overloaded method
        joined.writeAsCsv(params.get("output"), "\n", " ",FileSystem.WriteMode.OVERWRITE);
        leftOuterJoin.writeAsCsv(params.get("output"), "\n", " " , FileSystem.WriteMode.OVERWRITE);
        rightOuterjoin.writeAsCsv(params.get("output"), "\n", " ",FileSystem.WriteMode.OVERWRITE);
        fullOuterJoin.writeAsCsv(params.get("output"), "\n", " ",FileSystem.WriteMode.OVERWRITE);


        env.execute("Join example");
    }
}
