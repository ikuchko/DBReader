package com.illiakins;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB {
    private static final Logger LOG = LoggerFactory.getLogger(DB.class);
    private static Map<String, DataSource> dataSourceHashMap = new HashMap<>();
    private static Map<String, HikariConfig> configMap = new HashMap<>();

    public DB(String jdbcUrl, String userName, String password, int minimumIdle, int maxPoolSize,
            long leakDetectionThreshold, long connTimeout, long idleTimeout, long maxLifetime)
            throws HikariPool.PoolInitializationException {
        if (dataSourceHashMap.get("default") == null) {
            LOG.debug("CREATE NEW CONNECTION POOL");
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(userName);
            config.setPassword(password);
            config.setMinimumIdle(minimumIdle);
            config.setMaximumPoolSize(maxPoolSize);
            config.setAutoCommit(true);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.setLeakDetectionThreshold(leakDetectionThreshold); // example (2500) 2.5 seconds
            config.setConnectionTimeout(connTimeout); // example default(5000) 5 seconds
            config.setIdleTimeout(idleTimeout); // example (900_000) 15 minutes
            config.setMaxLifetime(maxLifetime); // example (28_440_000) 7.9 hours
            configMap.put("default", config);
            dataSourceHashMap.put("default", new HikariDataSource(config));
        }
    }

    public DB(String dataSourceName, String jdbcUrl, String userName, String password, int minimumIdle, int maxPoolSize,
            long leakDetectionThreshold, long connTimeout, long idleTimeout, long maxLifetime)
            throws HikariPool.PoolInitializationException {
        if (dataSourceHashMap.get(dataSourceName) == null) {
            LOG.debug("CREATE NEW CONNECTION POOL ({})", dataSourceName);
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(userName);
            config.setPassword(password);
            config.setMinimumIdle(minimumIdle);
            config.setMaximumPoolSize(maxPoolSize);
            config.setAutoCommit(true);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.setLeakDetectionThreshold(leakDetectionThreshold); // example (2500) 2.5 seconds
            config.setConnectionTimeout(connTimeout); // example default(5000) 5 seconds
            config.setIdleTimeout(idleTimeout); // example (900_000) 15 minutes
            config.setMaxLifetime(maxLifetime); // example (28_440_000) 7.9 hours
            configMap.put(dataSourceName, config);
            dataSourceHashMap.put(dataSourceName, new HikariDataSource(config));
        }
    }

    private static Map<String, DataSource> getDataSourceHashMap() {
        return dataSourceHashMap;
    }

    public static DataSource getDataSource(String dataSourceName) {
        return getDataSourceHashMap().get(dataSourceName);
    }

    public static DataSource getDataSource() {
        return getDataSource("default");
    }

    public static Map<String, HikariConfig> getConfigMap() {
        return configMap;
    }

    private static DataSource constructDataSource(String dataSourceName) throws SQLException {
        DataSource dataSource;
        if (!getConfigMap().containsKey(dataSourceName)) {
            throw new SQLException("No configuration found for the name: {}", dataSourceName);
        }
        dataSource = new HikariDataSource(getConfigMap().get(dataSourceName));
        getDataSourceHashMap().put(dataSourceName, dataSource);
        return dataSource;
    }

    private static Connection getConnection(String dataSourceName) throws SQLException {
        Connection conn;
        DataSource dataSource = getDataSource(dataSourceName);
        if (dataSource == null) {
            dataSource = constructDataSource(dataSourceName);
        }
        try {
            conn = dataSource.getConnection();
        } catch (SQLException sqlEx) {
            LOG.error("Unable to get connection from connection pool...", sqlEx);
            //last attempt to manually establish a connection
            dataSource = constructDataSource(dataSourceName);
            try {
                conn = dataSource.getConnection();
            } catch (SQLException e) {
                throw new SQLException("Second attempt to get a DB connection failed...", e);
            }
        }
        return conn;
    }

    public static Connection getConnectionForTransaction() throws SQLException {
        return getConnectionForTransaction("default");
    }

    public static Connection getConnectionForTransaction(String dataSourceName) throws SQLException {
        Connection connection = getConnection(dataSourceName);
        if (connection == null) {
            LOG.error("Can not retrieve a connection for `{}` dataSourceName", dataSourceName);
            return null;
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new SQLException("Error getting connection for transaction.", e);
        }
        return connection;
    }

    public static boolean releaseConnectionForTransaction(Connection connection) throws SQLException {
        return releaseConnectionForTransaction(connection, true);
    }

    public static boolean releaseConnectionForTransaction(Connection connection, Boolean makeCommit)
            throws SQLException {
        try {
            if (makeCommit) {
                connection.setAutoCommit(true);
                return true;
            }
        } catch (SQLException e) {
            throw new SQLException("Error releasing connection for transaction.", e);
        } finally {
            close(connection, null, null);
        }
        return false;
    }

    private static List<HashMap<String, Object>> resultSetToArrayList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List list = new ArrayList();
        while (rs.next()) {
            HashMap<String, Object> row = new HashMap(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            list.add(row);
        }

        return list;
    }

    public static List<HashMap<String, Object>> executeQuery(String dataSourceName, String sqlQuery)
            throws SQLException {
        List<HashMap<String, Object>> result;
        Connection connection = getConnection(dataSourceName);
        result = executeQuery(connection, sqlQuery);
        close(connection, null, null);
        return result;
    }

    public static List<HashMap<String, Object>> executeQuery(Connection connection, String sqlQuery)
            throws SQLException {
        if (connection == null) {
            connection = getConnection("default");
        }
        Statement statement = null;
        ResultSet resultSet = null;
        List result;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sqlQuery);
            result = resultSetToArrayList(resultSet);
        } finally {
            close(null, statement, resultSet);
        }

        return result;
    }

    public static List<HashMap<String, Object>> executeQuery(String sqlQuery) throws SQLException {
        return executeQuery("default", sqlQuery);
    }

    public static List<HashMap<String, Object>> executeQuery(String dataSourceName, String sqlQuery,
            List<Object> params) throws SQLException {
        List<HashMap<String, Object>> result;
        Connection connection = getConnection(dataSourceName);
        result = executeQuery(connection, sqlQuery, params);
        close(connection, null, null);
        return result;
    }

    public static List<HashMap<String, Object>> executeQuery(Connection connection, String sqlQuery,
            List<Object> params) throws SQLException {
        boolean closeConnection = false;
        if (connection == null) {
            connection = getConnection("default");
            closeConnection = true;
        }
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        List result;
        try {
            statement = connection.prepareStatement(sqlQuery);
            int parameterIndex = 1;
            for (Object param : params) {
                statement.setObject(parameterIndex++, param);
            }
            resultSet = statement.executeQuery();
            result = resultSetToArrayList(resultSet);
        } finally {
            close(closeConnection ? connection : null, statement, resultSet);
        }
        return result;
    }

    public static List<HashMap<String, Object>> executeQuery(String sqlQuery, List<Object> params) throws SQLException {
        return executeQuery("default", sqlQuery, params);
    }

    public static List<HashMap<String, Object>> executeQuery(String dataSourceName, String sqlQuery,
            Object... parameters) throws SQLException {
        return executeQuery(dataSourceName, sqlQuery, varargsToList(parameters));
    }

    public static List<HashMap<String, Object>> executeQuery(String sqlQuery, Object... parameters)
            throws SQLException {
        return executeQuery("default", sqlQuery, parameters);
    }

    public static List<HashMap<String, Object>> executeQuery(Connection connection, String sqlQuery,
            Object... parameters) throws SQLException {
        return executeQuery(connection, sqlQuery, varargsToList(parameters));
    }

    /**
     * Keeps the connection open.
     * Use for controlling transactions.
     * Connection must be closed in the end of all transactions by calling releaseConnectionForTransaction()
     */
    public static int executeUpdate(Connection connection, String sqlQuery, List<Object> params) throws SQLException {
        boolean closeConnection = false;
        if (connection == null) {
            connection = getConnection("default");
            closeConnection = true;
        }

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        int generatedKey = 0;
        try {
            statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
            int parameterIndex = 1;
            for (Object param : params) {
                statement.setObject(parameterIndex++, param);
            }
            int retval = statement.executeUpdate();
            resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                generatedKey = resultSet.getInt(1);
            }
            if (generatedKey == 0) {
                //If it was an update/delete statement return the # of row affected instead of the generated keys.
                generatedKey = retval;
            }
        } finally {
            close(closeConnection ? connection : null, statement, resultSet);
        }
        return generatedKey;
    }

    public static int executeUpdate(Connection connection, String sqlQuery) throws SQLException {
        return executeUpdate(connection, sqlQuery, new ArrayList<>());
    }

    public static int executeUpdate(Connection connection, String sqlQuery,  Object... parameters) throws SQLException {
        return executeUpdate(connection, sqlQuery, varargsToList(parameters));
    }

    public static int executeUpdate(String dataSourceName, String sqlQuery, List<Object> params) throws SQLException {
        Connection connection = getConnection(dataSourceName);
        int generatedKey = -1;
        if (connection != null) {
            try {
                generatedKey = executeUpdate(connection, sqlQuery, params);
            } finally {
                close(connection, null, null);
            }
        }
        return generatedKey;
    }

    public static int executeUpdate(String dataSourceName, String sqlQuery, Object... parameters) throws SQLException {
        return executeUpdate(dataSourceName, sqlQuery, varargsToList(parameters));
    }

    public static int executeUpdate(String dataSourceName, String sqlQuery) throws SQLException {
        return executeUpdate(dataSourceName, sqlQuery, new ArrayList<>());
    }

    public static int executeUpdate(String sqlQuery) throws SQLException {
        return executeUpdate("default", sqlQuery);
    }

    public static int executeUpdate(String sqlQuery, List<Object> params) throws SQLException {
        return executeUpdate("default", sqlQuery, params);
    }

    public static int executeUpdate(String sqlQuery, Object... parameters) throws SQLException {
        return executeUpdate("default", sqlQuery, parameters);
    }

    /**
     * Keeps the connection open.
     * Use for controlling transactions.
     * Connection must be closed in the end of all transactions by calling releaseConnectionForTransaction()
     */
    public static HashMap<String, Integer> executeUpdateBatch(Connection connection, String sqlQuery, String subQuery,
            List<List<Object>> paramList) throws SQLException {
        boolean closeConnection = false;
        if (connection == null) {
            connection = getConnection("default");
            closeConnection = true;
        }
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        HashMap<String, Integer> result = new HashMap<>();

        final StringBuilder builderPlaceholder = new StringBuilder("(");
        for (int i = 0; i < paramList.get(0).size(); i++) {
            if (i != 0) {
                builderPlaceholder.append(",");
            }
            builderPlaceholder.append("?");
        }
        String insertPlaceholders = builderPlaceholder.append(")").toString();
        final StringBuilder builder = new StringBuilder(sqlQuery);
        for (int i = 0; i < paramList.size(); i++) {
            if (i != 0) {
                builder.append(",");
            }
            builder.append(insertPlaceholders);
        }
        if (!subQuery.equals("")) {
            builder.append(" ").append(subQuery);
        }
        try {
            statement = connection.prepareStatement(builder.toString(), Statement.RETURN_GENERATED_KEYS);
            int parameterIndex = 1;
            for (List<Object> param : paramList) {
                for (Object value : param) {
                    statement.setObject(parameterIndex++, value);
                }
            }
            statement.execute();
            result.put("affected", statement.getUpdateCount());
            resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                result.put("id", resultSet.getInt(1));
            }
        } finally {
            close(closeConnection ? connection : null, statement, resultSet);
        }
        return result;
    }

    public static HashMap<String, Integer> executeUpdateBatch(String sqlQuery, String subQuery,
            List<List<Object>> paramList, String dataSourceName) throws SQLException {
        Connection connection = getConnection(dataSourceName);
        HashMap<String, Integer> result = new HashMap<>();
        if (connection != null) {
            try {
                result = executeUpdateBatch(connection, sqlQuery, subQuery, paramList);
            } finally {
                close(connection, null, null);
            }
        }
        return result;
    }

    public static HashMap<String, Integer> executeUpdateBatch(String sqlQuery, String subQuery,
            List<List<Object>> paramList) throws SQLException {
        return executeUpdateBatch(sqlQuery, subQuery, paramList,"default");
    }

    public static HashMap<String, Integer> executeUpdateBatch(String sqlQuery, List<List<Object>> paramList,
            String dataSourceName) throws SQLException {
        return executeUpdateBatch(sqlQuery, "", paramList, dataSourceName);
    }

    public static HashMap<String, Integer> executeUpdateBatch(String sqlQuery, List<List<Object>> paramList)
            throws SQLException {
        return executeUpdateBatch(sqlQuery, paramList, "default");
    }


    public static void execStoredProcedure(String dataSourceName, String procName, List<Object> params) throws SQLException {
        Connection connection = null;
        CallableStatement statement = null;
        try {
            connection = getConnection(dataSourceName);
            statement = connection.prepareCall("{ call " + procName + " }");
            int parameterIndex = 1;
            for (Object param : params) {
                statement.setObject(parameterIndex++, param);
            }
            statement.execute();
        } finally {
            close(connection, statement, null);
        }
    }

    public static void execStoredProcedure(String procName, List<Object> params) throws SQLException {
        execStoredProcedure("default", procName, params);
    }

    public static void execStoredProcedure(String dataSourceName, String procName, Object... parameters)
            throws SQLException {
        execStoredProcedure(dataSourceName, procName, varargsToList(parameters));
    }

    public static void execStoredProcedure(String procName, Object... parameters) throws SQLException {
        execStoredProcedure("default", procName, parameters);
    }

    public static List<Object> varargsToList(Object... parameters) {
        return new ArrayList<>(Arrays.asList(parameters));
    }

    public static void closeDBPool(String dataSourceName) throws SQLException {
        DataSource dataSource = getDataSourceHashMap().get(dataSourceName);
        if (dataSource != null) {
            dataSource.unwrap(HikariDataSource.class).close();
            getDataSourceHashMap().remove(dataSource);
        }
    }

    public static void closeDBPool() throws SQLException {
        closeDBPool("default");
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new SQLException("SQL error occurred during closing a connection", e);
        }
    }

}
