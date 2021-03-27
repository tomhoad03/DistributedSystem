import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static int fileFolder;

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = Integer.parseInt(args[3]); // location of data store

            // establish datastore listener
            ServerSocket datastoreSocket = new ServerSocket(datastorePort);

            // establish connection to controller
            DatastoreThread controllerThread = new DatastoreThread(new Socket(datastoreSocket.getInetAddress(), controllerPort));
            new Thread(controllerThread).start();
            controllerThread.joinController();

            try {
                for (;;) {
                    try {
                        // establish connection to client
                        final Socket clientSocket = datastoreSocket.accept();
                        new Thread(new DatastoreThread(clientSocket)).start();
                    } catch (Exception ignored) { }
                }
            } catch (Exception e) { System.out.println("Error: Invalid Socket!"); }
        } catch (Exception e) { System.out.println("Error: Invalid Arguments!"); }
    }

    static class DatastoreThread implements Runnable {
        public final Socket socket;
        public final BufferedReader in;
        public final PrintWriter out;
        public String line;

        public DatastoreThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream());
        }

        // basic listener
        public void run() {
            try {
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: Invalid Thread!");
            }
        }

        // establish connection to controller
        public void joinController() {
            out.println("JOIN " + socket.getPort());
            out.flush();
        }
    }
}