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

            ArrayList<Socket> dataStores = new ArrayList<>();

            try {
                // establish controller listener
                ServerSocket controllerSocket = new ServerSocket(controllerPort);
                for (;;) {
                    try {
                        // establish connection to client
                        Socket clientSocket = controllerSocket.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        String line;

                        // receive messages from client
                        while ((line = in.readLine()) != null) {

                            // establish connection to new datastore
                            if (line.startsWith("JOIN")) {
                                dataStores.add(new Socket("Tom-Laptop", Integer.parseInt(line.substring(5))));
                                PrintWriter out = new PrintWriter(dataStores.get(0).getOutputStream());

                                out.println("ACK");
                                out.flush();
                                System.out.println("ACK");
                            }
                        }
                        clientSocket.close();
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

 8. Launch in terminal, Ctrl-C to close running program
 9. java Controller.java 6400 0 0 0
 10. java Dstore.java 0 6400 0 0 0
 */