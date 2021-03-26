import java.io.PrintWriter;
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
                // establish connection to controller
                Socket socket = new Socket("Tom-Laptop", controllerPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream());

                // send messages to controller
                for(int i = 1; i < 10; i++) {
                    Thread.sleep(1000);

                    out.println("TCP message " + i);
                    out.flush();
                    System.out.println("TCP message " + i + " sent");
                }
            }
            catch (Exception e) {
                System.out.println("Error: Invalid Send!");
            }
        } catch (Exception e) {
            System.out.println("Error: Invalid Arguments!");
        }
    }
}