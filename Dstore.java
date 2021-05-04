import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Objects;

public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static String fileFolder;

    public static int refreshRate = 2;
    public static DstoreThread datastoreThread;
    public static DstoreLogger datastoreLogger;

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = args[3]; // location of data store

            ServerSocket datastoreSocket = new ServerSocket(datastorePort);
            datastoreLogger = new DstoreLogger(Logger.LoggingType./*ON_FILE_AND_TERMINAL*/ON_TERMINAL_ONLY, datastorePort);

            // establish connection to controller
            datastoreThread = new DstoreThread(new Socket(InetAddress.getLocalHost(), controllerPort));
            new Thread(datastoreThread).start();
            datastoreThread.joinController();

            // thread for establishing new connections to clients
            Thread socketThread = new Thread(() -> {
                for (;;) {
                    try {
                        // establish new connection to client
                        final Socket clientSocket = datastoreSocket.accept();
                        new Thread(new DstoreThread(clientSocket)).start();

                        Thread.sleep(refreshRate);
                    } catch (Exception e) {
                        datastoreLogger.log("Socket error (" + e + ")");
                    }
                }
            });
            socketThread.start();
        } catch (Exception e) {
            datastoreLogger.log("Server error (" + e + ")");
        }
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
                    datastoreLogger.messageReceived(socket, line);

                    if (line.startsWith("STORE ")) { // store operation
                        try {
                            String fileName = line.split(" ")[1];
                            String fileSize = line.split(" ")[2];

                            // send ack to client and get file contents
                            datastoreLogger.messageSent(socket, "ACK");
                            out.println("ACK");

                            byte[] fileContents = socket.getInputStream().readNBytes(Integer.parseInt(fileSize));
                            File file = new File(fileFolder + File.separator + fileName);
                            file.getParentFile().mkdirs();
                            file.createNewFile();
                            Files.write(file.toPath(), fileContents);

                            // send ack to controller
                            if (socket.getPort() == datastorePort || socket.getLocalPort() == datastorePort) {
                                datastoreLogger.messageSent(datastoreThread.socket, "STORE_ACK " + fileName);
                                datastoreThread.out.println("STORE_ACK " + fileName);
                            }
                        } catch (Exception e) {
                            datastoreLogger.log("Malformed store message from the client (" + line + ")");
                        }
                    } else if (line.startsWith("LOAD_DATA ")) { // load operation
                        boolean found = false;
                        try {
                            String fileName = line.split(" ")[1];

                            for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                if (file.getName().equals(fileName)) {
                                    socket.getOutputStream().write(Files.readAllBytes(file.toPath()));
                                    found = true;
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            datastoreLogger.log("Malformed load message from the client (" + line + ")");
                        }
                        if (!found) {
                            datastoreLogger.messageSent(datastoreThread.socket, "ERROR_FILE_DOES_NOT_EXIST");
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                        }
                    } else if (line.startsWith("REMOVE ")) {
                        boolean found = false;
                        try {
                            String fileName = line.split(" ")[1];

                            for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                                if (file.getName().equals(fileName)) {
                                    Files.delete(file.toPath());
                                    found = true;
                                    break;
                                }
                            }
                            if (found) {
                                datastoreLogger.messageSent(datastoreThread.socket, "REMOVE_ACK " + fileName);
                                datastoreThread.out.println("REMOVE_ACK " + fileName);
                            }
                        } catch (Exception e) {
                            datastoreLogger.log("Malformed remove message from the client (" + line + ")");
                        }
                    } else if (line.equals("LIST")) { // list operation
                        StringBuilder files = new StringBuilder();

                        for (File file : Objects.requireNonNull(new File(fileFolder).listFiles())) {
                            if (!(files.length() == 0)) {
                                files.append(" ");
                            }
                            files.append(file.getName());
                        }
                        datastoreLogger.messageSent(socket, String.valueOf(files));
                        out.println(files);
                    }
                }
            } catch (Exception e) {
                datastoreLogger.log("Operation error (" + e + ")");
            }
        }

        public void joinController() throws Exception {
            File folder = new File(fileFolder);
            if (!folder.exists()) {
                boolean mkdir = folder.mkdir();
                if (!mkdir) {
                    throw new Exception("Cannot join controller");
                }
            }
            datastoreLogger.messageSent(socket, "JOIN " + datastorePort);
            out.println("JOIN " + datastorePort);
        }
    }
}