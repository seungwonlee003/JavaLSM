import db.DB;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException {
        DB db = new DB();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Database CLI: Commands are 'put <key> <value>', 'get <key>', 'delete <key>', 'exit'");

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            String[] parts = input.split("\\s+", 3);

            if (parts.length == 0) {
                System.out.println("Invalid command");
                continue;
            }

            String command = parts[0].toLowerCase();
            try {
                switch (command) {
                    case "put":
                        if (parts.length != 3) {
                            System.out.println("Usage: put <key> <value>");
                            break;
                        }
                        db.put(parts[1], parts[2]);
                        System.out.println("OK");
                        break;

                    case "get":
                        if (parts.length != 2) {
                            System.out.println("Usage: get <key>");
                            break;
                        }
                        String value = db.get(parts[1]);
                        System.out.println((value == null || "<TOMBSTONE>".equals(value)) ? "Not found" : value);
                        break;

                    case "delete":
                        if (parts.length != 2) {
                            System.out.println("Usage: delete <key>");
                            break;
                        }
                        db.delete(parts[1]);
                        System.out.println("OK");
                        break;

                    case "display":
                        db.display();
                        break;

                    case "exit":
                        System.out.println("Exiting...");
                        scanner.close();
                        return;

                    default:
                        System.out.println("Unknown command. Use 'put', 'get', 'delete', or 'exit'");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}