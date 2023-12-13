package com.ivoyant;

import com.ivoyant.mysql.*;
import com.mysql.cj.xdevapi.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DatabaseOperations {
    Map<String, Connection> connectionMap = new HashMap<>();
    static Scanner scanner = new Scanner(System.in);
    private String key;

    // Creating connection
    public void connectionRequest() {
        System.out.print("Please Enter Host Name : ");
        String hostName = scanner.nextLine();
        System.out.print("Please Enter Port Number : ");
        int portNumber = scanner.nextInt();
        scanner.nextLine();
        System.out.print("Please Enter Database Name : ");
        String databaseName = scanner.nextLine();
        System.out.print("Please Enter Username : ");
        String username = scanner.nextLine();
        System.out.print("Please Enter Password : ");
        String password = scanner.nextLine();
        key = username + "_" + databaseName;
        if (connectionMap.containsKey(key)) {
            System.out.println("Already Connected");
        } else {
            connectionMap.put(key, ConnectionHandler.connect(hostName, portNumber, databaseName, username, password));
        }
    }

    // Looping for operations
    public void performOperations() {
        try {
            if (connectionMap.get(key) != null && !connectionMap.get(key).isClosed()) {
                System.out.println("Connection Successful");
                while (true) {
                    System.out.print("Please Select The Operation You Need To Perform : \n1. Show Tables\n" +
                            "2. Create Table\n3. Insert Data\n4. Display Data\n5. Delete Data\n6. Drop Table\n" +
                            "7. Close Connection And Exit\n");
                    int operationSelected = scanner.nextInt();
                    scanner.nextLine();
                    switch (operationSelected) {
                        case 1:
                            showTables();
                            break;
                        case 2:
                            createTable();
                            break;
                        case 3:
                            insertData();
                            break;
                        case 4:
                            displayData();
                            break;
                        case 5:
                            deleteData();
                            break;
                        case 6:
                            dropTable();
                            break;
                        case 7:
                            if (ConnectionHandler.disconnect(connectionMap.get(key)))
                                System.out.println("Disconnected\nThank You :)");
                            else {
                                connectionMap.get(key).close();
                                System.out.println("Disconnected\nThank You :)");
                            }
                            return;
                        default:
                            System.out.println("Invalid Operation Chosen...");
                            break;
                    }
                }
            } else {
                System.out.println("Connection Failed");
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    // Creating Table
    public void createTable() {
        System.out.print("Table Name : ");
        String tableName = scanner.nextLine();
        System.out.print("Column Names With Datatypes And Constraints : ");
        String columnNames = scanner.nextLine();
        if (TableOperations.createTable(connectionMap.get(key), tableName, columnNames) == -1) {
            System.out.println("Error Occured While Creating Table Named " + tableName);
        }
    }

    // Displaying Table
    public void showTables() {
        ResultSet resultSet = TableOperations.showTables(connectionMap.get(key));
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    System.out.println("Table Name : " + resultSet.getString(1));
                }
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
            }
        }
    }

    // Drop Table
    public void dropTable() {
        System.out.print("Enter Table Name To Drop : ");
        String tableName = scanner.nextLine();
        if (TableOperations.dropTable(connectionMap.get(key), tableName)) {
            System.out.println(tableName + " Dropped Successfully");
        } else {
            System.out.println(tableName + " Drop Not Successful");
        }
    }

    // Insert Data
    public void insertData() {
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        System.out.print("Enter Column Names (Separated By ,): ");
        String columns = scanner.nextLine();
        System.out.print("Enter Values List (Separated By ,): ");
        String values = scanner.nextLine();
        int count = DataOperations.insertData(connectionMap.get(key), tableName, columns, values);
        if (count == -1) {
            System.out.println("Insertion UnSuccessful");
        } else {
            System.out.println("Inserted " + count + " Rows Successfully.");
        }
    }

    // Display Data
    public void displayData() {
        System.out.print("Enter Column List : ");
        String columnList = scanner.nextLine();
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        ResultSet resultSet = DataOperations.displayData(connectionMap.get(key), columnList, tableName);
        if (resultSet != null) {
            try {
                int columnCount = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getMetaData().getColumnName(i) + "\t");
                }
                System.out.println();
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                }
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
            }
        } else {
            System.out.println("Error Occured During Fetching Data!!");
        }
    }

    // Delete Data
    public void deleteData() {
        System.out.print("1. Without Where Clause\n2.With Clause\n");
        int option = scanner.nextInt();
        scanner.nextLine();
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        int deleteCount = -1;
        switch (option) {
            case 1:
                deleteCount = DataOperations.deleteData(connectionMap.get(key), tableName);
                break;
            case 2:
                System.out.print("Enter Where Condition : ");
                String condition = scanner.nextLine();
                deleteCount = DataOperations.deleteData(connectionMap.get(key), tableName, condition);
                break;
            default:
                System.out.println("Invalid Choice");
        }
        if (deleteCount == -1) {
            System.out.println("Deletion UnSuccessful");
        } else {
            System.out.println("Deleted " + deleteCount + " Rows Successfully.");
        }
    }
}