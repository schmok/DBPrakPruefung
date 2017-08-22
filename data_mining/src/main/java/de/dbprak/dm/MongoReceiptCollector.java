package de.dbprak.dm;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
/**
 * Created by viktorspadi on 12.07.17.
 */
public class MongoReceiptCollector implements IFPCollector{
    private MongoDatabase db;

    public MongoReceiptCollector() {
        try{

            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

            // Now connect to your databases
            MongoDatabase db = mongoClient.getDatabase( "aufg3" );
            System.out.println("Connect to database successfully");
            this.db = db;

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    public List<List<String>> getData() {
        if(db != null) {
            MongoCollection<Document> receiptsCollection = db.getCollection("receipts");

            FindIterable<Document> documents = receiptsCollection.find();
            long count = receiptsCollection.count();
            List<String>[] receipts = new List[(int) count];
            final int[] i = {0};
            documents.forEach((Consumer<? super Document>) document -> {
                if(document.containsKey("products")) {
                    receipts[i[0]] = new ArrayList<String>((Collection<? extends String>) ((List)document.get("products"))
                            .stream()
                            .map(o -> {
                                return ((Document)o).get("product_name").toString();
                            })
                            .collect(Collectors.toSet()));

                } else {
                    receipts[i[0]] = new ArrayList<>();
                }
                i[0]++;
            });

            return Arrays.asList(receipts);
        }
        return null;
    }
}
