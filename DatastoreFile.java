public class DatastoreFile {
    private String fileName;
    private String fileSize;
    private String fileLocation;

    // constructor for dstore file index
    public DatastoreFile(String fileName, String fileSize, String fileLocation) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileLocation = fileLocation;
    }

    // constructor for controller file index
    public DatastoreFile(String fileName, String fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFileSize() {
        return fileSize;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }
}
