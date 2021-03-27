import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

@SuppressWarnings("InfiniteLoopStatement")
public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static int fileFolder;

    public static DstoreThread dstoreThread;

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = Integer.parseInt(args[3]); // location of data store

            // datastore socket
            ServerSocket datastoreSocket = new ServerSocket(datastorePort);

            // establish new connection to controller
            dstoreThread = new DstoreThread(new Socket(datastoreSocket.getInetAddress(), controllerPort));
            new Thread(dstoreThread).start();
            dstoreThread.joinController();

            try {
                for (;;) {
                    try {
                        // establish new connection to client
                        final Socket clientSocket = datastoreSocket.accept();
                        new Thread(new DstoreThread(clientSocket)).start();
                    } catch (Exception ignored) { }
                }
            } catch (Exception e) { System.out.println("Error: Invalid Socket!"); }
        } catch (Exception e) { System.out.println("Error: Invalid Arguments!"); }
    }

    static class DstoreThread implements Runnable {
        public final Socket socket;
        public final BufferedReader in;
        public final PrintWriter out;
        public String line;

        public DstoreThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream());
        }

        // basic listener
        public void run() {
            try {
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("STORE ")) { // store operation
                        String filename = line.split(" ")[1];
                        String filesize = line.split(" ")[2];

                        // send ack to client
                        out.println("ACK");
                        out.flush();

                        // get file contents from client
                        for (;;) {
                            try {
                                while ((line = in.readLine()) != null) {
                                    String fileContents = line;

                                    // store contents of file

                                    // sends ack to controller
                                    dstoreThread.out.println("STORE_ACK " + filename);
                                    dstoreThread.out.flush();
                                    break;
                                }
                            } catch (Exception e) {
                                System.out.println("Error: Invalid ACK Send!");
                            }
                        }
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: Invalid Thread!");
            }
        }

        // establish connection to controller
        public void joinController() {
            out.println("JOIN " + datastorePort);
            out.flush();
        }
    }
}