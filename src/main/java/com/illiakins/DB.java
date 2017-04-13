package com.illiakins;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DB {
	private static final Logger LOG = Logger.getLogger(DB.class);
	private static DataSource dataSource;

	public DB(String jdbcUrl, String userName, String password, int minimumIdle, int maxPoolSize, long leakDetectionThreshold, long connTimeout, long idleTimeout, long maxLifetime) throws HikariPool.PoolInitializationException {
		if (dataSource == null) {
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
			dataSource = new HikariDataSource(config);
		}
	}

	public static synchronized DataSource getDataSource() {
		return dataSource;
	}

	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = getDataSource().getConnection();
		} catch (SQLException sqlEx) {
			LOG.fatal("Unable to get connection from connection pool...", sqlEx);
			//last attempt to manually establish a connection
			try {
				Class.forName(PropertiesReader.getStringSetting("DB_DRIVER_CLASS"));
				conn = DriverManager.getConnection(
						PropertiesReader.getStringSetting("DB_URL"),
						PropertiesReader.getStringSetting("DB_USERNAME"),
						PropertiesReader.getStringSetting("DB_PASSWORD"));

				return conn;
			} catch (Exception ex) {
				LOG.fatal("Error connecting to the database...", ex);
			}
		}
		return conn;
	}
	private static List<HashMap<String, Object>> resultSetToArrayList(ResultSet rs) throws SQLException{
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();
		List list = new ArrayList();
		while (rs.next()){
			HashMap<String, Object> row = new HashMap(columns);
			for(int i=1; i<=columns; ++i){
				row.put(md.getColumnLabel(i),rs.getObject(i));
			}
			list.add(row);
		}

		return list;
	}

	public static List<HashMap<String, Object>> executeQuery(String sqlQuery) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		List result;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);
			result = resultSetToArrayList(resultSet);
		} finally {
			closeConnection(connection, statement, resultSet);
		}

		return result;
	}

	public static List<HashMap<String, Object>> executeQuery(String sqlQuery, List<Object> params) throws SQLException {
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List result;
		try {
			connection = getConnection();
			statement = connection.prepareStatement(sqlQuery);
			int parameterIndex = 1;
			for (Object param : params) {
				statement.setObject(parameterIndex++, param);
			}
			resultSet = statement.executeQuery();
			result = resultSetToArrayList(resultSet);
		} finally {
			closeConnection(connection, statement, resultSet);
		}

		return result;
	}
	
	public static int executeUpdate(String sqlQuery) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		int generatedKey = 0;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			statement.executeUpdate(sqlQuery, Statement.RETURN_GENERATED_KEYS);
			resultSet = statement.getGeneratedKeys();
			if (resultSet.next()) {
				generatedKey = resultSet.getInt(1);
			}
		} finally {
			closeConnection(connection, statement, resultSet);
		}
		return generatedKey;
	}

	public static int executeUpdate(String sqlQuery, Boolean updating) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		int amountOfUpdatedRows = 0;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			amountOfUpdatedRows = statement.executeUpdate(sqlQuery);
		} finally {
			closeConnection(connection, statement, null);
		}
		return amountOfUpdatedRows;
	}

	public static int executeUpdate(String sqlQuery, List<Object> params) throws SQLException {
		Connection connection = null;
		PreparedStatement statement = null;
        ResultSet resultSet = null;
        int generatedKey = 0;
		try {
			connection = getConnection();
			statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
			int parameterIndex = 1;
			for (Object param : params) {
				statement.setObject(parameterIndex++, param);
			}
			statement.executeUpdate();
            resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                generatedKey = resultSet.getInt(1);
            }
		} finally {
			closeConnection(connection, statement, resultSet);
		}
		return generatedKey;
	}

	public static int executeUpdateBatch(String sqlQuery, String subQuery, List<List<Object>> paramList) throws SQLException {
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int id = 0;

		final StringBuilder builderPlaceholder = new StringBuilder("(");
		for ( int i = 0; i < paramList.get(0).size(); i++ ) {
			if ( i != 0 ) {
				builderPlaceholder.append(",");
			}
			builderPlaceholder.append("?");
		}
		String insertPlaceholders = builderPlaceholder.append(")").toString();
		final StringBuilder builder = new StringBuilder(sqlQuery);
		for ( int i = 0; i < paramList.size(); i++ ) {
			if ( i != 0 ) {
				builder.append(",");
			}
			builder.append(insertPlaceholders);
		}
		if (!subQuery.equals("")) builder.append(" ").append(subQuery);
		try {
			connection = getConnection();
			statement = connection.prepareStatement(builder.toString(), Statement.RETURN_GENERATED_KEYS);
			int parameterIndex = 1;
			for (List<Object> param : paramList) {
				for (Object value : param) {
					statement.setObject(parameterIndex++, value);
				}
			}
			statement.execute();
			resultSet = statement.getGeneratedKeys();
			if (resultSet.next()) {
				id = resultSet.getInt(1);
			}
		} catch (SQLException e) {
			LOG.error("SQL error occurred", e);
		} finally {
			closeConnection(connection, statement, null);
		}
		return id;
	}

	public static int executeUpdateBatch(String sqlQuery, List<List<Object>> paramList) throws SQLException {
		return executeUpdateBatch(sqlQuery, "", paramList);
	}

	public static void execStoredProcedure(String procName, List<Object> params) throws SQLException {
		Connection connection = null;
		CallableStatement statement = null;
		try {
			connection = getConnection();
			statement = connection.prepareCall("{ call " + procName + " }");
			int parameterIndex = 1;
			for (Object param : params) {
				statement.setObject(parameterIndex++, param);
			}
			statement.execute();
		} finally {
			closeConnection(connection, statement, null);
		}
	}

    public static void closeConnection(Connection connection, Statement statement, ResultSet resultSet) {
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
            LOG.error("SQL error occurred", e);
        }
    }

}
