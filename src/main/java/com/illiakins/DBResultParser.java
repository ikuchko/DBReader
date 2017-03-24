package com.illiakins;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Illiak on 11/22/2016.
 */
public class DBResultParser {
    private HashMap<String, Object> resultRow;

    public DBResultParser(HashMap<String, Object> resultRow) {
        this.resultRow = resultRow;
    }

    public Integer getInt(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column with name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return Integer.valueOf(resultRow.get(key).toString());
    }

    public String getString(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column with name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return String.valueOf(resultRow.get(key));
    }

    public Boolean getBool(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column with name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return (Boolean.valueOf(resultRow.get(key).toString()) || resultRow.get(key).toString().equals("1"));
    }

    public HashMap<String, Object> getResultHashMap() {
        return resultRow;
    }

    public static List<DBResultParser> getResultSet(List<HashMap<String, Object>> dbResult) {
        List<DBResultParser> resultList = new ArrayList<>();
        dbResult.forEach(resultDB -> resultList.add(new DBResultParser(resultDB)));
        return resultList;
    }
}
