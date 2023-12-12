package com.ivoyant;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        DatabaseOperations databaseOperations = new DatabaseOperations();
        System.out.println("Hello and welcome! to Employee Management System");
        databaseOperations.connectionRequest();
        databaseOperations.performOperations();
    }
}
