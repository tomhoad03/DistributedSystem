import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static String fileFolder;

    public static int refreshRate = 2;
    public static DstoreThread dstoreThread;

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = args[3]; // location of data store

            // establish datastore listener
            ServerSocket datastoreSocket = new ServerSocket(datastorePort);

            // establish connection to controller
            dstoreThread = new DstoreThread(new Socket(InetAddress.getLocalHost(), controllerPort));
            new Thread(dstoreThread).start();
            dstoreThread.joinController();

            // thread for receiving new clients
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish new connection to client
                        final Socket clientSocket = datastoreSocket.accept();
                        new Thread(new DstoreThread(clientSocket)).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception e) { System.out.println("Socket Error: " + e);}
                }
            });
            socketThread.start();
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

        public void run() {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (Exception e) { System.out.println("Operation Error: " + e); }
        }

        public void joinController() throws Exception {
            File folder = new File(fileFolder);
            if (!folder.exists()) {
                boolean mkdir = folder.mkdir();
                if (!mkdir) {
                    throw new Exception("Cannot join controller");
                }
            }
            out.println("JOIN X" /*+ datastorePort*/);
        }
    }
}