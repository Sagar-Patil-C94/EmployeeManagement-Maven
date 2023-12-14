package com.ivoyant;

import com.ivoyant.mysql.CustomLogger;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        DatabaseOperations databaseOperations = new DatabaseOperations();
        CustomLogger.info("Starting Employee Management System.....");
        System.out.println("Hello and welcome! to Employee Management System");
        databaseOperations.chooseDatabaseType();
        databaseOperations.performOperations();
    }
}
