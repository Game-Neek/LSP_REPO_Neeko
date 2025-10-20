package org.howard.edu.lsp.midterm.question4;

public abstract class Device {
  private String id;
  private String location;
  private long lastHeartbeatEpochSeconds;
  private boolean connected;

  // PROVIDED CONSTRUCTOR
  public Device(String id, String location) {
    if (id == null || id.isEmpty() || location == null || location.isEmpty()) {
      throw new IllegalArgumentException("Invalid id or location");
    }
    this.id = id;
    this.location = location;
    this.lastHeartbeatEpochSeconds = 0;
    this.connected = false;
  }

  public String getId() {
    return id;
  }

  public String getLocation() {
    return location;
  }



  public long getLastHeartbeatEpochSeconds() {
    return lastHeartbeatEpochSeconds;
  }

  public boolean isConnected() {
    return connected;
  }

  protected void setConnected(boolean connected) {
    this.connected = connected;
  }

  public void heartbeat() {
    this.lastHeartbeatEpochSeconds = System.currentTimeMillis() / 1000;
  }

  public abstract String getStatus();
}


