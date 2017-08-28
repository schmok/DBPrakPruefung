package de.dbprak.dm.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Florian on 22.08.2017.
 *
 * Source: https://stackoverflow.com/a/32232173
 */
public class JdbcUtil {
    public static <R> Stream<R> getResultsAsStream(Connection connection, QueryResultWrapper<R> queryResultWrapper, int fetchSize) throws SQLException {
        UncheckedCloseable close = null;
        try {
            close = UncheckedCloseable.wrap(connection);
            PreparedStatement pSt = connection.prepareStatement(queryResultWrapper.getSql());
            close = close.nest(pSt);
            connection.setAutoCommit(false);
            pSt.setFetchSize(fetchSize);
            ResultSet resultSet = pSt.executeQuery();
            close = close.nest(resultSet);
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<R>(Long.MAX_VALUE, Spliterator.ORDERED) {
                @Override
                public boolean tryAdvance(Consumer<? super R> action) {
                    try {
                        if (!resultSet.next()) return false;
                        action.accept(queryResultWrapper.createRecord(resultSet));
                        return true;
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }, false).onClose(close);
        } catch (SQLException sqlEx) {
            if (close != null)
                try {
                    sqlEx.printStackTrace();
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
