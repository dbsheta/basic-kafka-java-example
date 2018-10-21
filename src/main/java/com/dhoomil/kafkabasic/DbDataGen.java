package com.dhoomil.kafkabasic;

import java.sql.*;

public class DbDataGen {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String url = "jdbc:postgresql://localhost:5432/test_db?user=postgres&password=admin";
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url);
        PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM movies");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString(3));
        }
    }
}
