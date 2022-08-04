package com.meesho.prismdatabricksapi.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import com.meesho.prismdatabricksapi.configs.*;

public class SCIMConnectionWrapper {
    public Connection PostgresConnector(String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Properties p = new Properties();
        Connection con = DriverManager.getConnection(url, username, password);
        Statement statement = con.createStatement();
        if (con != null) {
            System.out.println("Connection is established successfully to Metastore using Postgres JDBC Connector");
        } else {
            System.out.println("Connection is not established successfully to Metastore using Postgres JDBC Connector");
            System.exit(1);
        }
        return con;

    }
    public Integer UpdateRecordforSCIM(Connection con, String sql_statement){
        Statement statement = null;
        try {
            statement = con.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Integer update_record_status;
        try {
            update_record_status = statement.executeUpdate(sql_statement);
            System.out.println("Record is updated successfully for the SCIM User");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
      return update_record_status;
    }

    public Integer InsertRecordforSCIM(Connection con,String sql_statement){
        Statement statement = null;
        try {
            statement = con.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Integer record_insert_status;
        try {
            record_insert_status = statement.executeUpdate(sql_statement);
            System.out.println("Record is inserted successfully for the SCIM User");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return record_insert_status;
    }

    public void ExecuteStatementforSCIM(Connection con, String sql_statement) throws SQLException {
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery(sql_statement);
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        System.out.println("Columns used to get ResultSet from Service Principal SCIM_User Table");
        for (int j = 1; j <= columnsNumber; j++) {
            System.out.println(rsmd.getColumnName(j).toUpperCase()+ " ");
        }

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        SCIMConnectionWrapper connection_obj = new SCIMConnectionWrapper();
        ApplicationProperties properties = new ApplicationProperties();
        String metastore_host = properties.getValue("metastore_host");
        String metastore_username = properties.getValue("metastore_username");
        String metastore_password = properties.getValue("metastore_password");
        Connection connection = connection_obj.PostgresConnector(metastore_host, metastore_username, metastore_password);

    }
}