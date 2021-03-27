import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Controller {
    public static void main(String[] args) {
        try {
            // reading arguments
            int controllerPort = Integer.parseInt(args[0]);  // port to listen on
            int replicationFactor = Integer.parseInt(args[1]); // replication factor
            int timeout = Integer.parseInt(args[2]); // timeout wait time
            int rebalancePeriod = Integer.parseInt(args[3]); // rebalance wait time

            ArrayList<Socket> dataStores = new ArrayList<>(); // list of all datastore sockets

            try {
                // establish controller listener
                ServerSocket controllerSocket = new ServerSocket(controllerPort);
                for (;;) {
                    try {
                        // establish connection to client
                        final Socket clientSocket = controllerSocket.accept();

                        // multithreading
                        new Thread(() -> {
                            try {
                                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                                PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
                                String line;

                                // receive messages from client / datastores
                                while ((line = in.readLine()) != null) {
                                    if (line.startsWith("JOIN")) { // establish connection to new datastore
                                        dataStores.add(new Socket(clientSocket.getInetAddress(), Integer.parseInt(line.substring(5))));
                                    } else if (dataStores.size() < replicationFactor) { // disallow client connections
                                        break;
                                    } else if (line.startsWith("STORE")) { // store operation
                                        System.out.println("Store");
                                    } else if (line.startsWith("LOAD")) { // load operation
                                        System.out.println("Load");
                                    } else if (line.startsWith("REMOVE")) { // remove operation
                                        System.out.println("Remove");
                                    } else if (line.startsWith("LIST")) { // list operation
                                        System.out.println("List");
                                    }
                                }
                                clientSocket.close();
                            } catch (Exception e) {
                                System.out.println("Error: Invalid Thread!");
                            }
                        }).start();
                    } catch (Exception ignored) { }
                }
            } catch (Exception e) { System.out.println("Error: Invalid Socket!"); }
        } catch (Exception e) { System.out.println("Error: Invalid Arguments!"); }
    }
}
/*
 Notes:
 1. Client is given, create controller and dstores

 2. Store - client stores files in every datastore
 3. Load - controller gives client requested file (from one datastore)
 4. Remove - file removed from every datastore
 5. List - gets a list of all the files
 6. Storage Rebalancing - each file is stored in r dstores and files are evenly stored (when adding a new dstore and after interval)

 7. Each process gets logged (more info later)

 8. Launch in terminal, Ctrl-C to close running program, remove .java with class files
 9. java Controller.java 6400 1 0 0
 10. java Dstore.java 6401 6400 0 0
 */