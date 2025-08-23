package com.cloudera.opensearch;

public class IndexData {
    private String firstName;
    private String lastName;

    public IndexData() {
        super();
    }

    public IndexData(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return String.format("IndexData{first name='%s', last name='%s'}", firstName, lastName);
    }
}
