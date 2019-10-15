package com.aliyun.openservices.odps.console.utils;

import org.jline.terminal.Terminal;

import com.aliyun.openservices.odps.console.utils.jline.ODPSLineReader;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * This class is a helper class to manage signal handlers.
 *
 * Since odps console uses Jline3 to handle console input, signals are handled by Jline3 when
 * reading a line and they are handled by user-defined handlers when processing a line. This class
 * is used to manage user-defined signal handlers.
 */
public class SignalUtil {

  public static void registerSignalHandler(String signalName, SignalHandler handler) {

    if (ODPSConsoleUtils.isWindows()) {
      Signal.handle(new Signal(signalName), handler);
    } else {
      Terminal.Signal jlineSignal = Terminal.Signal.valueOf(signalName);
      Terminal.SignalHandler jlineSignalHandler =
          signal1 -> handler.handle(new Signal(signalName));

      ODPSLineReader.getInstance().registerSignalHandler(jlineSignal, jlineSignalHandler);
    }
  }

  public static SignalHandler getInstanceRunningIntSignalHandler(Thread currentThread) {
    return signal -> {
      registerSignalHandler("INT", getDefaultIntSignalHandler(currentThread));
      currentThread.interrupt();
    };
  }

  public static SignalHandler getDefaultIntSignalHandler(Thread currentThread) {
    return signal -> {
      if (currentThread.isInterrupted()) {
        System.exit(128 + new Signal("INT").getNumber());
      } else {
        currentThread.interrupt();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          //do nothing
        }
        if (currentThread.isInterrupted()) {
          System.err.println("Press Ctrl-C again to exit ODPS console");
        }
      }
    };
  }
}
