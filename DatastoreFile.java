public class DatastoreFile {
    private String fileName;
    private String fileSize;
    private String fileContents;

    // constructor for dstore file index
    public DatastoreFile(String fileName, String fileSize, String fileContents) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileContents = fileContents;
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

    public String getFileContents() {
        return fileContents;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public void setFileContents(String fileContents) {
        this.fileContents = fileContents;
    }
}
