package de.dbprak.dm;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by viktorspadi on 12.07.17.
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

            String getReceiptsSQL = "SELECT rp.RECEIPT_ID, LISTAGG(rp.PRODUCT_ID, ',') WITHIN GROUP (ORDER BY rp.PRODUCT_ID) AS ITEM_IDS, LISTAGG(p.PRODUCT_NAME, ',') WITHIN GROUP (ORDER BY rp.PRODUCT_ID) AS ITEM_NAMES\n" +
                    "FROM AUFG2_ETL.RECEIPT_PRODUCT rp, AUFG2_ETL.PRODUCT p\n" +
                    "WHERE rp.PRODUCT_ID = p.ID\n" +
                    "GROUP BY RECEIPT_ID";


            Stream<String> results = resultSetAsStream(conn, getReceiptsSQL, "ITEM_NAMES");
            long endTimeGetData = System.currentTimeMillis();
            System.out.println("Time for executing query:" + (endTimeGetData - startTimeGetData) + "ms");
            final List<List<String>> receiptItemList = results
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
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }

        return new ArrayList<>();
    }

    private static Stream<String> resultSetAsStream(Connection connection, String sql, String resStringColumnName) throws SQLException {

        UncheckedCloseable close = null;
        try {
            close = UncheckedCloseable.wrap(connection);
            PreparedStatement pSt = connection.prepareStatement(sql);
            close = close.nest(pSt);
            connection.setAutoCommit(false);
            pSt.setFetchSize(5000);
            ResultSet resultSet = pSt.executeQuery();
            close = close.nest(resultSet);
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<String>(Long.MAX_VALUE, Spliterator.ORDERED) {
                @Override
                public boolean tryAdvance(Consumer<? super String> action) {
                    try {
                        if (!resultSet.next()) return false;
                        action.accept(resultSet.getString(resStringColumnName));
                        return true;
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }, false).onClose(close);
        } catch (SQLException sqlEx) {
            if (close != null)
                try {
                    close.close();
                } catch (Exception ex) {
                    sqlEx.addSuppressed(ex);
                }
            throw sqlEx;
        }
    }

    interface UncheckedCloseable extends Runnable, AutoCloseable {
        default void run() {
            try {
                close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        static UncheckedCloseable wrap(AutoCloseable c) {
            return c::close;
        }

        default UncheckedCloseable nest(AutoCloseable c) {
            return () -> {
                try (UncheckedCloseable c1 = this) {
                    c.close();
                }
            };
        }
    }
}
