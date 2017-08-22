package de.dbprak.dm;

import de.dbprak.dm.util.JdbcUtil;
import de.dbprak.dm.util.QueryResultObject;
import de.dbprak.dm.util.QueryResultWrapper;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author fdecker
 */
public class SqlReceiptCollector implements IFPCollector{
    public List<List<String>> getData() {
        try {

            long startTimeGetData = System.currentTimeMillis();

            String url = "jdbc:oracle:thin:@134.106.56.17:1521:dbprak";
            Connection conn = DriverManager.getConnection(url, "AUFG2_ETL", "dbprakwise16");
            Statement stmt = conn.createStatement();

            //ResultSet countReceiptsResultSet = stmt.executeQuery("SELECT COUNT(DISTINCT RECEIPT_ID) AS TOTAL FROM RECEIPT_PRODUCT");
            //countReceiptsResultSet.next();
            //int receiptsCount = countReceiptsResultSet.getInt("TOTAL");

            //language=SQL
            String getReceiptsSQL = "SELECT rp.RECEIPT_ID, LISTAGG(rp.PRODUCT_ID, ',') WITHIN GROUP (ORDER BY rp.PRODUCT_ID) AS ITEM_IDS, LISTAGG(p.PRODUCT_NAME, ',') WITHIN GROUP (ORDER BY rp.PRODUCT_ID) AS ITEM_NAMES\n" +
                    "FROM AUFG2_ETL.RECEIPT_PRODUCT rp, AUFG2_ETL.PRODUCT p\n" +
                    "WHERE rp.PRODUCT_ID = p.ID\n" +
                    "GROUP BY RECEIPT_ID";

            QueryResultObject<String> queryResultObject =  new QueryResultObject<String>() {
                @Override
                public String createRecord(ResultSet resultSet) throws SQLException {
                    return resultSet.getString("ITEM_NAMES");
                }
            };

            QueryResultWrapper<String> queryResultWrapper = new QueryResultWrapper<>(getReceiptsSQL, queryResultObject);

            try(Stream<String> results = JdbcUtil.getResultsAsStream(conn, queryResultWrapper)) {
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
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }

        return new ArrayList<>();
    }
}
