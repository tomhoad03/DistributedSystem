import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings({"InfiniteLoopStatement", "ResultOfMethodCallIgnored"})
public class Dstore {
    public static int datastorePort;
    public static int controllerPort;
    public static int timeout;
    public static String fileFolder;

    public static DatastoreThread datastoreThread; // controller connection

    public static ArrayList<String> datastoreFileNames = new ArrayList<>(); // list of files in datastore

    public static void main(String[] args) {
        try {
            // reading arguments
            datastorePort = Integer.parseInt(args[0]); // port to listen on
            controllerPort = Integer.parseInt(args[1]); // controller port
            timeout = Integer.parseInt(args[2]); // timeout wait time
            fileFolder = args[3]; // location of data store

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

                        // create new file
                        File file = new File(fileFolder + File.separator + fileName);
                        file.getParentFile().mkdirs();
                        file.createNewFile();

                        // write to file
                        BufferedWriter writer = new BufferedWriter(new FileWriter(file.getPath()));
                        writer.write(fileContents);
                        writer.close();

                        datastoreFileNames.add(fileName);

                        // send ack to controller
                        if (socket.getPort() == datastorePort || socket.getLocalPort() == datastorePort) {
                            datastoreThread.sendMsg("STORE_ACK " + fileName);
                        }

                    } else if (line.startsWith("LOAD_DATA ")) {
                        String fileName = line.split(" ")[1];
                        boolean found = false;

                        // gets the file from the datastore folder
                        for (String datastoreFileName : datastoreFileNames) {
                            if (datastoreFileName.equals(fileName)) {
                                File file = new File(fileFolder + File.separator + datastoreFileName);
                                sendMsg(new String(Files.readAllBytes(Paths.get(file.getPath()))));
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            sendMsg("ERROR_FILE_DOES_NOT_EXIST");
                        }

                    } else if (line.startsWith("REMOVE ")) {
                        String fileName = line.split(" ")[1];
                        boolean found = false;

                        // removes the file
                        for (String datastoreFileName : datastoreFileNames) {
                            if (datastoreFileName.equals(fileName)) {
                                Files.delete(Paths.get(fileFolder + File.separator + datastoreFileName));
                                datastoreFileNames.remove(datastoreFileName);
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            datastoreThread.sendMsg("REMOVE_ACK " + fileName);
                        } else {
                            sendMsg("ERROR_FILE_DOES_NOT_EXIST " + fileName);
                        }

                    } else if (line.equals("LIST")) {
                        StringBuilder files = new StringBuilder();

                        // gets the list of files
                        File folder = new File(fileFolder);
                        File[] fileList = folder.listFiles();
                        assert fileList != null;
                        datastoreFileNames.clear();

                        // looks for files in the folder
                         for (File file : fileList) {
                             datastoreFileNames.add(file.getName());
                        }

                        // builds a list of all the known files
                        for (String datastoreFileName : datastoreFileNames) {
                            if (!(files.length() == 0)) {
                                files.append(" ");
                            }
                            files.append(datastoreFileName);
                        }
                        String toSend = files.toString();
                        sendMsg(toSend);

                    } else if (line.startsWith("REBALANCE ")) {
                        ArrayList<String> splitLine = new ArrayList<>(Arrays.asList(line.split(" ")));
                        int numSends = Integer.parseInt(splitLine.get(1));
                        int count = 2;

                        // file sending
                        for (int i = 0; i <= numSends - 1; i++) {
                            String fileName = splitLine.get(count);
                            int numPorts = Integer.parseInt(splitLine.get(count + 1));
                            ArrayList<String> ports = new ArrayList<>(splitLine.subList(count + 2, count + numPorts + 2));

                            // send file to ports
                            for (String port : ports) {
                                Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(port));
                                BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                                String dstoreLine;

                                File file = new File(fileFolder + File.separator + fileName);

                                dstoreOut.println("STORE " + fileName + " " + file.length());

                                while ((dstoreLine = dstoreIn.readLine()) != null) {
                                    if (dstoreLine.equals("ACK")) {
                                        BufferedReader reader = new BufferedReader(new FileReader(file.getPath()));
                                        StringBuilder fileContents = new StringBuilder();
                                        String readerLine;

                                        // reads the contents of the file
                                        while ((readerLine = reader.readLine()) != null) {
                                            if (fileContents.length() == 0) {
                                                fileContents.append(readerLine);
                                            } else {
                                                fileContents.append(File.separator).append(readerLine);
                                            }
                                        }
                                        reader.close();

                                        dstoreOut.println(fileContents);
                                        break;
                                    }
                                }
                                dstoreSocket.close();
                            }
                            count = count + numPorts + 2;
                        }

                        // file removing
                        if (Integer.parseInt(splitLine.get(count)) > 0) {
                            ArrayList<String> toRemove = new ArrayList<>(splitLine.subList(count + 1, splitLine.size()));

                            for (String file : toRemove) {
                                for (String datastoreFileName : datastoreFileNames) {
                                    if (datastoreFileName.equals(file)) {
                                        Files.delete(Paths.get(fileFolder + File.separator + datastoreFileName));
                                        datastoreFileNames.remove(datastoreFileName);
                                        break;
                                    }
                                }
                            }
                        }
                        sendMsg("REBALANCE COMPLETE");
                    }
                }
                socket.close();
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }

        // establish connection to controller
        public void joinController() {
            File folder = new File(fileFolder);
            if (!folder.exists()) {
                folder.mkdir();
            }

            out.println("JOIN " + datastorePort);
        }

        // sends a message to the client
        public void sendMsg(String msg) {
            out.println(msg);
        }

        // sends a message to the client, expects a response
        public String sendMsgReceiveMsg(String msg) throws Exception {
            out.println(msg);
            return in.readLine();
        }
    }
}