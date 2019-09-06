//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class Spark_Test {
//    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("sparkhello").setMaster("local[6]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> textFile = sc.textFile("F:/spark_file/word.txt");
//        JavaRDD<String> filter = textFile.filter(s->s.contains("h"));
//        JavaRDD<String> flatMap = filter.flatMap(s-> {
//
//                ArrayList<String> list = new ArrayList<>();
//                String[] s1 = s.split(" ");
//                for (int i = 0; i < s1.length; i++) {
//                    list.add(s1[i]);
//                }
//                return list.iterator();
//        });
//        JavaRDD<Tuple2<String, Integer>> map = flatMap.map(s-> new Tuple2<>(s, 1));
//        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> group = map.groupBy(s->s._1);
//        List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> list = group.collect();
//        JavaPairRDD<String, Integer> mapValues = group.mapValues(s->{
//                Integer sum = 0;
//                for (Tuple2<String, Integer> tup : s
//                ) {
//                    sum += tup._2;
//                }
//                return sum;
//        });
//        System.out.println(mapValues.collect());
//
//    }
//}
