package de.dbprak.dm.util;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fdecker
 */
public class QueryResultWrapper<T>  {
    private String sql;
    private QueryResultObject<T> queryResultObject;

    public QueryResultWrapper(String sql, QueryResultObject<T> queryResultObject) {
        this.sql = sql;
        this.queryResultObject = queryResultObject;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public QueryResultObject<T> getQueryResultObject() {
        return queryResultObject;
    }

    public void setQueryResultObject(QueryResultObject<T> queryResultObject) {
        this.queryResultObject = queryResultObject;
    }

    public T createRecord(ResultSet resultSet) throws SQLException {
        return queryResultObject.createRecord(resultSet);
    }
}
