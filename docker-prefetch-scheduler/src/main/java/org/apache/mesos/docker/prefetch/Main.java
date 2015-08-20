package org.apache.mesos.docker.prefetch;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Main entry point for the Scheduler.
 */
public final class Main {

  private final Log log = LogFactory.getLog(Main.class);

  public static void main(String[] args) {
    new Main().start();
  }

  private void start() {
    Injector injector = Guice.createInjector();
    getSchedulerThread(injector).start();
  }


  private Thread getSchedulerThread(Injector injector) {
    Thread scheduler = new Thread(injector.getInstance(PrefetchScheduler.class));
    scheduler.setName("HdfsScheduler");
    scheduler.setUncaughtExceptionHandler(getUncaughtExceptionHandler());
    return scheduler;
  }

  private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {

    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        log.error("Scheduler exiting due to uncaught exception", e);
        System.exit(2);
      }
    };
  }

  /**
   * Exceptions in the scheduler which likely result in the scheduler being shutdown.
   */
  public static class
    SchedulerException extends RuntimeException {

    public SchedulerException(Throwable cause) {
      super(cause);
    }

    public SchedulerException(String message) {
      super(message);
    }

    public SchedulerException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
