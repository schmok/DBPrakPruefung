package de.dbprak.dm.pruefung;

import de.dbprak.dm.fp.FrequentPatternMining;
import de.dbprak.dm.fp.IFPCollector;
import de.dbprak.dm.util.JdbcUtil;
import de.dbprak.dm.util.QueryResultObject;
import de.dbprak.dm.util.QueryResultWrapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author fdecker
 */
public class PruefungFrequentPatternMiningUrl {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaAssociationRulesExample").setMaster("local[2]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        analyzeDataOnSQLOracle( sc);
    }


    static void analyzeDataOnSQLOracle(JavaSparkContext sc){
        IFPCollector ifpCollector = new IFPCollector() {
            @Override
            public List<List<String>> getData() {
                try{
                    long startTimeGetData = System.currentTimeMillis();

                    String url = "jdbc:oracle:thin:@134.106.56.17:1521:dbprak";
                    Connection conn = DriverManager.getConnection(url, "PRUEFUNG", "dbprakwise16");
                    Statement stmt = conn.createStatement();

                    //language=SQL
                    String getReceiptsSQL =
                            "SELECT ID, LISTAGG(URL, ',') WITHIN GROUP (ORDER BY URL) AS URLS\n" +
                                    "FROM (" +
                                    "SELECT ID, URL FROM ACCESS_LOG WHERE REFERRER IS NOT NULL\n" +
                                    "UNION\n" +
                                    "SELECT ID, REFERRER as URL FROM ACCESS_LOG WHERE REFERRER IS NOT NULL\n" +
                                    ") GROUP BY ID";

                    QueryResultObject<String> queryResultObject =  new QueryResultObject<String>() {
                        @Override
                        public String createRecord(ResultSet resultSet) throws SQLException {
                            return resultSet.getString("URLS");
                        }
                    };

                    QueryResultWrapper<String> queryResultWrapper = new QueryResultWrapper<>(getReceiptsSQL, queryResultObject);

                    try(Stream<String> results = JdbcUtil.getResultsAsStream(conn, queryResultWrapper, 10000)) {
                        long endTimeGetData = System.currentTimeMillis();
                        System.out.println("Time for executing query:" + (endTimeGetData - startTimeGetData) + "ms");
                        final List<List<String>> receiptItemList = results.parallel()
                                .map(s -> {
                                    Set<String> set = new HashSet<>();
                                    Collections.addAll(set, s.split(","));
                                    return new ArrayList<>(set);
                                })
                                .collect(Collectors.toList());

                        long endTimeStringToList = System.currentTimeMillis();
                        System.out.println("Time for endTimeStringToList:" + (endTimeStringToList - endTimeGetData) + "ms");
                        long endTimeProcessData = System.currentTimeMillis();
                        System.out.println("Time for endTimeProcessData:" + (endTimeProcessData - startTimeGetData) + "ms");
                        conn.close();

                        return receiptItemList;
                    }
                } catch (Exception e) {
                    System.err.println("Got an exception! " + e.getMessage());
                }

                return new ArrayList<>();
            }
        };

        FrequentPatternMining.analyze(sc, ifpCollector, 0.01, 0.1,2, 1);
    }

}