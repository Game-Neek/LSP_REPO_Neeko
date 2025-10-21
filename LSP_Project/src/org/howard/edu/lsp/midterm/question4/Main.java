package org.howard.edu.lsp.midterm.question4;

import java.util.*;

public class Main {
  public static void main(String[] args) {
    Device lock   = new DoorLock("DL-101", "DormA-1F", 85);
    Device thermo = new Thermostat("TH-202", "Library-2F", 21.5);
    Device cam    = new Camera("CA-303", "Quad-North", 72);

    // === Invalid battery test ===
    System.out.println("\n== Exception test ==");
    try {
      Device badCam = new Camera("CA-404", "Test-Lab", -5);
      System.out.println("ERROR: Exception was not thrown for invalid battery!");
    } catch (IllegalArgumentException e) {
      System.out.println("Caught expected exception: " + e.getMessage());
    }

    // === Heartbeat demonstration ===
    System.out.println("\n== Heartbeat timestamps BEFORE ==");
    for (Device d : Arrays.asList(lock, thermo, cam)) {
      System.out.println(d.getId() + " lastHeartbeat=" + d.getLastHeartbeatEpochSeconds());
    }

    lock.heartbeat();
    thermo.heartbeat();
    cam.heartbeat();

    System.out.println("\n== Heartbeat timestamps AFTER ==");
    for (Device d : Arrays.asList(lock, thermo, cam)) {
      System.out.println(d.getId() + " lastHeartbeat=" + d.getLastHeartbeatEpochSeconds());
    }

    // === Base-class polymorphism ===
    List<Device> devices = Arrays.asList(lock, thermo, cam);
    System.out.println("\n== Initial status via Device ==");
    for (Device d : devices) {
      System.out.println(d.getStatus());
    }

    // === Interface polymorphism: Networked ===
    System.out.println("\n== Connect all Networked ==");
    for (Device d : devices) {
      if (d instanceof Networked) {
        ((Networked) d).connect();
      }
    }

    // === Interface polymorphism: BatteryPowered ===
    System.out.println("\n== Battery report (BatteryPowered) ==");
    for (Device d : devices) {
      if (d instanceof BatteryPowered) {
        BatteryPowered bp = (BatteryPowered) d;
        System.out.println(d.getClass().getSimpleName() + " battery = " + bp.getBatteryPercent() + "%");
      }
    }

    // === Final status check ===
    System.out.println("\n== Updated status via Device ==");
    for (Device d : devices) {
      System.out.println(d.getStatus());
    }
  }
}
