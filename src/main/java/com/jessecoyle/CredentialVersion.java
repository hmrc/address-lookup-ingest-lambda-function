package com.jessecoyle;

public class CredentialVersion {
    private String name;
    private String version;

    public CredentialVersion(String name, String version) {
        this.name = name;
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CredentialVersion{");
        sb.append("name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
