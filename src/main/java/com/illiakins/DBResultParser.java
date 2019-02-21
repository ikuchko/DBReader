package com.illiakins;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

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
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return Integer.valueOf(resultRow.get(key).toString());
    }

    public Double getDouble(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return Double.valueOf(resultRow.get(key).toString());
    }

    public String getString(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return String.valueOf(resultRow.get(key));
    }

    public Boolean getBool(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return (Boolean.valueOf(resultRow.get(key).toString()) || resultRow.get(key).toString().equals("1"));
    }

    public LocalDate getLocalDate(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return LocalDate.parse(resultRow.get(key).toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public LocalDate getLocalDate(String key, DateTimeFormatter dateTimeFormatter) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return LocalDate.parse(resultRow.get(key).toString(), dateTimeFormatter);
    }

    public LocalDateTime getLocalDateTime(String key) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return LocalDateTime.parse(resultRow.get(key).toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
    }

    public LocalDateTime getLocalDateTime(String key, DateTimeFormatter dateTimeFormatter) throws SQLException {
        if (!resultRow.containsKey(key)) {
            throw new SQLException("ResultSet does not contain column name '" + key +"'");
        }
        if (resultRow.get(key) == null || resultRow.get(key).equals("")) return null;
        return LocalDateTime.parse(resultRow.get(key).toString(), dateTimeFormatter);
    }

    public static Integer getCount(List<HashMap<String, Object>> dbResult, String columnName) throws SQLException {
        List<DBResultParser> resultSet = getResultSet(dbResult);
        if (resultSet.size() == 0 || resultSet.size() > 1 || !resultSet.get(0).getResultHashMap()
                .containsKey(columnName)) {
            return null;
        }
        return resultSet.get(0).getInt(columnName);
    }

    public HashMap<String, Object> getResultHashMap() {
        return resultRow;
    }

    public static List<DBResultParser> getResultSet(List<HashMap<String, Object>> dbResult) {
        List<DBResultParser> resultList = new ArrayList<>();
        dbResult.forEach(resultDB -> resultList.add(new DBResultParser(resultDB)));
        return resultList;
    }

    /**
     * Returns *Integer* of the first found column which contains "count" in the name OR the first available column
     * @param dbResult the list of HashMaps with a result of requested data from a DataBase
     * @return *Integer* of the first found column which contains "count" in the name OR the first available column
     * @throws SQLException if resultSet is empty or contains more than 1 record
     */
    public static Integer getCount(List<HashMap<String, Object>> dbResult) throws SQLException {
        List<DBResultParser> resultSet = getResultSet(dbResult);
        if (resultSet.size() == 0 || resultSet.size() > 1) {
            throw new SQLException("ResultSet is empty or contains more then 1 record");
        }
        List<String> keys = resultSet.get(0).getResultHashMap().keySet().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        String key = keys.get(0);
        key = keys.stream().filter(s -> s.contains("count")).findFirst().orElse(key);
        return resultSet.get(0).getInt(key);
    }

    /**
     * Returns a first record from the resultSet if available, or NULL otherwise
     * @param dbResult the list of HashMaps with a result of requested data from a DataBase
     * @return first record from the resultSet if available, or NULL otherwise
     */
    public static DBResultParser getFirstRecord(List<HashMap<String, Object>> dbResult) {
        List<DBResultParser> resultSet = getResultSet(dbResult);
        if (resultSet.size() > 0) {
            return resultSet.get(0);
        } else {
            return null;
        }
    }
}
