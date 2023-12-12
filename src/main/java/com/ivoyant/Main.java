package com.ivoyant;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    static Connection connection;
    static Scanner scanner = new Scanner(System.in);

    // Creating connection
    public static void connectionRequest() {
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
        connection = CreateConnection.connect(hostName, portNumber, databaseName, username, password);
    }

    // Looping for operations
    public static void performOperations() {
        try {
            if (connection != null && !connection.isClosed()) {
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
                            connection.close();
                            System.out.println("Thank You :)");
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
            e.printStackTrace();
        }
    }

    // Creating Table
    public static void createTable() {
        System.out.print("Table Name : ");
        String tableName = scanner.nextLine();
        System.out.print("Column Names With Datatypes And Constraints : ");
        String columnNames = scanner.nextLine();
        if (CreateTable.create(connection, tableName, columnNames) == -1) {
            System.out.println("Error Occured While Creating Table Named " + tableName);
        }
    }

    // Displaying Table
    public static void showTables() {
        ResultSet resultSet = ShowTables.show(connection);
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    System.out.println("Table Name : " + resultSet.getString(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Drop Table
    public static void dropTable() {
        System.out.print("Enter Table Name To Drop : ");
        String tableName = scanner.nextLine();
        if (DropTable.drop(connection, tableName)) {
            System.out.println(tableName + " Dropped Successfully");
        } else {
            System.out.println(tableName + " Drop Not Successful");
        }
    }

    // Insert Data
    public static void insertData() {
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        System.out.print("Enter Column Names (Separated By ,): ");
        String columns = scanner.nextLine();
        System.out.print("Enter Values List (Separated By ,): ");
        String values = scanner.nextLine();
        int count = InsertData.insert(connection, tableName, columns, values);
        if (count == -1) {
            System.out.println("Insertion UnSuccessful");
        } else {
            System.out.println("Inserted " + count + " Rows Successfully.");
        }
    }

    // Display Data
    public static void displayData() {
        System.out.print("Enter Column List : ");
        String columnList = scanner.nextLine();
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        ResultSet resultSet = DisplayData.display(connection, columnList, tableName);
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
    public static void deleteData() {
        System.out.print("1. Without Where Clause\n2.With Clause\n");
        int option = scanner.nextInt();
        scanner.nextLine();
        System.out.print("Enter Table Name : ");
        String tableName = scanner.nextLine();
        int deleteCount = -1;
        switch (option) {
            case 1:
                deleteCount = DeleteData.delete(connection, tableName);
                break;
            case 2:
                System.out.print("Enter Where Condition : ");
                String condition = scanner.nextLine();
                deleteCount = DeleteData.delete(connection, tableName, condition);
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

    public static void main(String[] args) throws SQLException {
        System.out.println("Hello and welcome! to Employee Management System");
        connectionRequest();
        performOperations();
    }
}