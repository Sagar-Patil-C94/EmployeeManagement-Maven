package com.ivoyant;


import com.ivoyant.mysql.ConnectionHandler;
import com.ivoyant.mysql.CustomLogger;
import com.ivoyant.mysql.DataOperations;
import com.ivoyant.mysql.TableOperations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DatabaseOperations {
    Map<String, Connection> connectionMap = new HashMap<>();
    static Scanner scanner = new Scanner(System.in);
    private String key;
    public String databaseType;

    public void chooseDatabaseType() {
        System.out.print("Please SQL Type\n1.MySQL\n2.Postgres : ");
        int choice = scanner.nextInt();
        scanner.nextLine();
        switch (choice) {
            case 1:
                databaseType = "mysql";
                CustomLogger.info("User Selected " + databaseType);
                connectionRequest();
                break;
            case 2:
                databaseType = "postgresql";
                CustomLogger.info("User Selected " + databaseType);
                connectionRequest();
                break;
            default:
                CustomLogger.error("User Selected Invalid Option");
//                System.out.println("Invalid choice");
        }
    }

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
            CustomLogger.warn("Connection Already Present!!!");
//            System.out.println("Already Connected");
        } else {
            connectionMap.put(key, ConnectionHandler.connect(hostName, databaseType, portNumber, databaseName, username, password));
            CustomLogger.info("Connection Established!!!");
        }
    }

    // Looping for operations
    public void performOperations() {
        try {
            if (connectionMap.get(key) != null && !connectionMap.get(key).isClosed()) {
//                System.out.println("Connection Successful");
                while (true) {
                    System.out.print("Please Select The Operation You Need To Perform : \n1. Show Tables\n" +
                            "2. Create Table\n3. Insert Data\n4. Display Data\n5. Delete Data\n6. Drop Table\n" +
                            "7. Close Connection And Exit\n");
                    CustomLogger.warn("Only Integer is Accepted");
                    int operationSelected = scanner.nextInt();
                    CustomLogger.info("User Selected Option " + operationSelected);
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
                            if (ConnectionHandler.disconnect(connectionMap.get(key))) {
                                CustomLogger.info("Disconnecting...!!!");
                                System.out.println("Disconnected\nThank You :)");
                            } else {
                                CustomLogger.info("Disconnecting...!!!");
                                connectionMap.get(key).close();
                                System.out.println("Disconnected\nThank You :)");
                            }
                            return;
                        default:
                            CustomLogger.error("User Selected Invalid Option");
//                            System.out.println("Invalid Operation Chosen...");
                            break;
                    }
                }
            } else {
                CustomLogger.error("Unable To Establish Function...!!!");
//                System.out.println("Connection Failed");
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

    // Creating Table
    public void createTable() {
        System.out.print("Table Name : ");
        String tableName = scanner.nextLine();
        CustomLogger.info("User Selected Table " + tableName);
        System.out.print("Column Names With Datatypes And Constraints : ");
        String columnNames = scanner.nextLine();
        if (TableOperations.createTable(connectionMap.get(key), tableName, columnNames) == -1) {
            CustomLogger.error("Error Occured While Creating Table Named " + tableName);
//            System.out.println("Error Occured While Creating Table Named " + tableName);
        }
    }

    // Displaying Table
    public void showTables() {
        ResultSet resultSet = TableOperations.showTables(connectionMap.get(key), databaseType);
        if (resultSet != null) {
            try {
                CustomLogger.info("Displaying Tables!!!");
                int columnIndex;
                if (databaseType == "mysql")
                    columnIndex = 1;
                else
                    columnIndex = 2;
                while (resultSet.next()) {
                    System.out.println("Table Name : " + resultSet.getString(columnIndex));
                }
            } catch (Exception e) {
                CustomLogger.error(e.getLocalizedMessage());
//                System.out.println(e.getLocalizedMessage());
            }
        }
    }

    // Drop Table
    public void dropTable() {
        System.out.print("Enter Table Name To Drop : ");
        String tableName = scanner.nextLine();
        CustomLogger.warn("Droping " + tableName);
        if (TableOperations.dropTable(connectionMap.get(key), tableName)) {
            CustomLogger.info(tableName + " Dropped Successfully");
//            System.out.println(tableName + " Dropped Successfully");
        } else {
            CustomLogger.error(tableName + " Drop Not Successful");
//            System.out.println(tableName + " Drop Not Successful");
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
            CustomLogger.error("Insertion UnSuccessful");
//            System.out.println("Insertion UnSuccessful");
        } else {
            CustomLogger.info("Inserted " + count + " Rows Successfully.");
//            System.out.println("Inserted " + count + " Rows Successfully.");
        }
    }

    // Display Data
    public void displayData() {
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        System.out.print("Enter Column List : ");
        String columnList = scanner.nextLine();
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
                CustomLogger.error(e.getLocalizedMessage());
//                System.out.println(e.getLocalizedMessage());
            }
        } else {
            CustomLogger.error("Error Occured During Fetching Data!!");
//            System.out.println("Error Occured During Fetching Data!!");
        }
    }

    // Delete Data
    public void deleteData() {
        System.out.print("1. Without Where Clause\n2.With Clause\n");
        int option = scanner.nextInt();
        scanner.nextLine();
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        CustomLogger.warn("Deleting From " + tableName);
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