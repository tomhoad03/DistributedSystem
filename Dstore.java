import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

@SuppressWarnings({"InfiniteLoopStatement"})
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
            dstoreThread = new DstoreThread(new Socket(InetAddress.getLocalHost(), controllerPort));
            new Thread(dstoreThread).start();
            dstoreThread.joinController();

            for (;;) {
                try {
                    // establish new connection to client
                    final Socket clientSocket = datastoreSocket.accept();
                    new Thread(new DstoreThread(clientSocket)).start();
                } catch (Exception ignored) { }
            }
        } catch (Exception e) { System.out.println("Error: " + e); }
    }

    static class DstoreThread implements Runnable {
        private final Socket socket;
        private final BufferedReader in;
        private final PrintWriter out;

        public DstoreThread(Socket socket) throws Exception {
            this.socket = socket;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
        }

        // basic listener
        public void run() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("STORE ")) { // store operation
                        String filename = line.split(" ")[1];
                        String filesize = line.split(" ")[2];

                        // send ack to client and get file contents
                        String fileContents = sendMsgReceiveMsg("ACK");

                        // store file contents
                        System.out.println(fileContents);

                        // send ack to controller
                        dstoreThread.sendMsg("STORE_ACK " + filename);
                        break;
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // establish connection to controller
        public void joinController() {
            out.println("JOIN " + datastorePort);
            out.flush();
        }

        public void sendMsg(String msg) {
            out.println(msg);
        }

        public String sendMsgReceiveMsg(String msg) throws Exception {
            out.println(msg);
            return in.readLine();
        }
    }
}