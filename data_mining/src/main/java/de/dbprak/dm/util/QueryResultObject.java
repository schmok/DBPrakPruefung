package de.dbprak.dm.util;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fdecker
 */
public interface QueryResultObject<R> {
    R createRecord(ResultSet resultSet) throws SQLException;
}
