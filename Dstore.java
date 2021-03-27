import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
    public static void main(String[] args) {
        try {
            // reading arguments
            int datastorePort = Integer.parseInt(args[0]); // port to listen on
            int controllerPort = Integer.parseInt(args[1]); // controller port
            int timeout = Integer.parseInt(args[2]); // timeout wait time
            int fileFolder = Integer.parseInt(args[3]); // location of data store

            try {
                // establish datastore listener
                ServerSocket datastoreSocket = new ServerSocket(datastorePort);

                // establish connection to controller
                Socket socket = new Socket("Tom-Laptop", controllerPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream());

                // join controller
                out.println("JOIN " + datastorePort);
                out.flush();
                System.out.println("JOIN " + datastorePort);

                for (;;) {
                    try {
                        // establish connection to client
                        Socket clientSocket = datastoreSocket.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        String line;

                        // receive messages from client
                        while ((line = in.readLine()) != null) {
                            System.out.println(line + " received");
                        }
                        clientSocket.close();
                    } catch (Exception ignored) { }
                }
            } catch (Exception e) { System.out.println("Error: Invalid Socket!"); }
        } catch (Exception e) { System.out.println("Error: Invalid Arguments!"); }
    }
}