package kuznetsov;

import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter name of the first DataBase:");
        String dataBase1 = scanner.nextLine();
        System.out.println("Enter IP of the first host:");
        String host1 = scanner.nextLine();
        System.out.printf("Enter username for %s:", dataBase1);
        String username1 = scanner.nextLine();
        System.out.printf("Enter password for %s:\n", dataBase1);
        String password1 = scanner.nextLine();

        System.out.println("Enter name of the second DataBase:");
        String dataBase2 = scanner.nextLine();
        System.out.println("Enter IP of the first host:");
        String host2 = scanner.nextLine();
        System.out.printf("Enter username for %s:", dataBase2);
        String username2 = scanner.nextLine();
        System.out.printf("Enter password for %s:\n", dataBase2);
        String password2 = scanner.nextLine();

        System.out.println("Enter names of fields separated by comma without spaces:");
        String fields = scanner.next();

        List<String> columns = Arrays.asList(fields.split(","));

        try {
            Class.forName("org.postgresql.Driver");

            Connection conn1 = DriverManager.getConnection("jdbc:postgresql://" + host1
                    + ":5432/" + dataBase1, username1, password1);
            Connection conn2 = DriverManager.getConnection("jdbc:postgresql://" + host2
                    + ":5432/" + dataBase2, username2, password2);

            Statement stmt1 = conn1.createStatement();
            Statement stmt2 = conn2.createStatement();

            ResultSet rs1 = stmt1.executeQuery("SELECT " + columns + " FROM " + dataBase1);
            ResultSet rs2 = stmt2.executeQuery("SELECT " + columns + " FROM " + dataBase2);

            List db1 = convertRStoList(rs1, columns);
            List db2 = convertRStoList(rs2, columns);

            System.out.println(compare(db1, db2));

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String>> convertRStoList(ResultSet rs, List<String> columns) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        IntStream streamI = IntStream.range(1, countColumns(rs));
        streamI.forEach(i -> {
            try {
                if (rs.next()) {
                    Map<String, String> map = new HashMap<>();
                    list.add(map);
                    IntStream streamJ = IntStream.range(1, columns.size());
                    streamJ.forEach(j -> {
                        try {
                            map.put(columns.get(j), rs.getString(j + 1));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return list;
    }

    private static int countColumns(ResultSet rs) throws SQLException {
        rs.last();
        int size = rs.getRow();
        return size;
    }

    private static boolean compare(List db1, List db2) {
        return db1.equals(db2);
    }
}
