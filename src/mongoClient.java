import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.MapReduceAction;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * Created by Celestial on 5/19/17.
 */
public class mongoClient {

    public static void main(String[] args) {



        //1.Establish a connection to MongoDB
        //Note: Mongo class is superseded by MongoClient in 3.4

        MongoClient client = new MongoClient();

        //Step 2. Point to the DB you want to access
        //Note: DB class is superseded by MongoDatabase

        MongoDatabase movieLens = client.getDatabase("movieLens");

        //Step 3. Get a reference to the Collection you want
        //Note: DBCollection is superseded by MongoCollection

        MongoCollection<Document> ratings = movieLens.getCollection("ratings");

//        MongoCursor<Document> cursor = ratings.find().iterator();
//        try {
//            while (cursor.hasNext()) {
//                System.out.println(cursor.next().toJson());
//            }
//        } finally {
//            cursor.close();
//        }
        

        String mapper1 = "function (){\n" +
                "emit(this.MovieID, {\"cnt\" : 1, \"rating\" : this.Rating });\n" +
                "}";
        String reducer1 = "function (key,values){\n" +
                "\tratingAvg = { \"cnt\": 0, \"rating\" : 0 };\n" +
                "\n" +
                "\tvalues.forEach(function(value) {\n" +
                "\t\tratingAvg.cnt += value.cnt;\n" +
                "        ratingAvg.rating += value.rating;\n" +
                "    });\n" +
                "\n" +
                "    return ratingAvg;\n" +
                "}";
        String finalizer1 = "function (key, ratingAvg) {\n" +
                "    ratingAvg.avg = ratingAvg.rating/ratingAvg.cnt;\n" +
                "    return ratingAvg;\n" +
                "}";

        ratings.mapReduce(mapper1, reducer1)
                .collectionName("result_top25") //指定结果保存的collection，默认为inline模式，不保存，直接通过cursor输出
                .action(MapReduceAction.REPLACE) //结果保存方式，如果”result“表存在，则替换。
                .nonAtomic(false) //是否为”非原子性“
                .sharded(false) //结果collection是否为sharded
                .finalizeFunction(finalizer1)
                .iterator() //触发执行，这句话千万别忘了，否则不会执行
                .close(); //我们不需要cursor，则直接关闭

    }



}
