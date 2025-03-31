package org.vividus.tika;

import java.io.Serializable;

public class MetaData implements Serializable {

    private String fileName;
    private String author;
    private String title;
    private String contentType;

    public MetaData(String fileName, String author, String title, String contentType ){
        this.fileName=fileName;
        this.author=author;
        this.title=title;
        this.contentType=contentType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String toString() {
        return "MetaData{" +
                "fileName='" + fileName + '\'' +
                ", author='" + author + '\'' +
                ", title='" + title + '\'' +
                ", contentType='" + contentType + '\'' +
                '}';
    }
}
