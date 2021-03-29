import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

@SuppressWarnings({"InfiniteLoopStatement"})
public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static int fileFolder;

    public static DatastoreThread datastoreThread; // controller connection

    public static ArrayList<DatastoreFile> datastoreFiles = new ArrayList<>();; // list of files in datastore

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
            datastoreThread = new DatastoreThread(new Socket(InetAddress.getLocalHost(), controllerPort));
            new Thread(datastoreThread).start();
            datastoreThread.joinController();

            for (;;) {
                try {
                    // establish new connection to client
                    final Socket clientSocket = datastoreSocket.accept();
                    new Thread(new DatastoreThread(clientSocket)).start();
                } catch (Exception ignored) { }
            }
        } catch (Exception e) { System.out.println("Error: " + e); }
    }

    static class DatastoreThread implements Runnable {
        private final Socket socket;
        private final BufferedReader in;
        private final PrintWriter out;

        public DatastoreThread(Socket socket) throws Exception {
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
                        String fileName = line.split(" ")[1];
                        String fileSize = line.split(" ")[2];

                        // send ack to client and get file contents
                        String fileContents = sendMsgReceiveMsg("ACK");

                        // store file contents
                        System.out.println(fileContents);
                        datastoreFiles.add(new DatastoreFile(fileName, fileSize, fileContents));

                        // send ack to controller
                        datastoreThread.sendMsg("STORE_ACK " + fileName);
                        stop();

                    } else if (line.startsWith("LOAD_DATA ")) {
                        String fileName = line.split(" ")[1];

                        // gets the file from the datastore folder
                        for (DatastoreFile datastoreFile : datastoreFiles) {
                            if (datastoreFile.getFileName().equals(fileName)) {
                                sendMsg(datastoreFile.getFileContents());
                            }
                        }
                        stop();
                    }
                }
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // establish connection to controller
        public void joinController() {
            out.println("JOIN " + datastorePort);
            out.flush();
        }

        // closes the current socket
        public void stop() throws Exception {
            in.close();
            out.close();
            socket.close();
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