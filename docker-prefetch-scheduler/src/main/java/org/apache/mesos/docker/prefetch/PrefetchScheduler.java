package org.apache.mesos.docker.prefetch;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Mesos Framework Scheduler class implementation.
 */
public class PrefetchScheduler implements org.apache.mesos.Scheduler, Runnable {
  private final Log log = LogFactory.getLog(PrefetchScheduler.class);

  private static final int SECONDS_FROM_MILLIS = 1000;
  private String mesosMasterUri;
  private String cmd;
  private List<Resource> executorResources;


  @Inject
  public PrefetchScheduler() {
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
    int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
      + slaveID.getValue() + " status=" + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
    byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
      + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {

  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {

  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {

  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId=" + slaveId.getValue());
  }

  @Override
  public void run() {
    FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
      .setName("name")
      .setCheckpoint(true);

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this,
      frameworkInfo.build(), getMesosMasterUri());
    driver.run();
  }

  private boolean launchNode(SchedulerDriver driver, Offer offer,
    String nodeName, List<String> taskTypes, String executorName) {
    // nodeName is the type of executor to launch
    // executorName is to distinguish different types of nodes
    // taskType is the type of task in mesos to launch on the node
    // taskName is a name chosen to identify the task in mesos and mesos-dns (if used)
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
      taskTypes.toString()));
    String taskIdName = String.format("%s.%s.%d", nodeName, executorName,
      System.currentTimeMillis());
    List<Resource> resources = getExecutorResources();
    ExecutorInfo executorInfo = createExecutor(taskIdName, nodeName, executorName, resources);
    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskType : taskTypes) {
      List<Resource> taskResources = getTaskResources(taskType);
      String taskName = getNextTaskName(taskType);
      TaskID taskId = TaskID.newBuilder()
        .setValue(String.format("task.%s.%s", taskType, taskIdName))
        .build();
      TaskInfo task = TaskInfo.newBuilder()
        .setExecutor(executorInfo)
        .setName(taskName)
        .setTaskId(taskId)
        .setSlaveId(offer.getSlaveId())
        .addAllResources(taskResources)
        .setData(ByteString.copyFromUtf8(
          String.format("bin/hdfs-mesos-%s", taskType)))
        .build();
      tasks.add(task);

    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
    return true;
  }

  private String getNextTaskName(String taskType) {
    return null;
  }

  private List<Resource> getTaskResources(String taskType) {
    return null;
  }


  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName,
    List<Resource> resources) {


    return ExecutorInfo
      .newBuilder()
      .setName(nodeName + " executor")
      .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
      .addAllResources(resources)
      .setCommand(
        CommandInfo
          .newBuilder()
          .setEnvironment(Environment.newBuilder()
            .addAllVariables(Arrays.asList(
              Environment.Variable.newBuilder()
                .setName("LD_LIBRARY_PATH")
                .setValue("**lib path**").build()
            )))
          .setValue(getCmd()).build())
      .build();
  }


  public void sendMessageTo(SchedulerDriver driver, TaskID taskId,
    SlaveID slaveID, String message) {
    log.info(String.format("Sending message '%s' to taskId=%s, slaveId=%s", message,
      taskId.getValue(), slaveID.getValue()));
    String postfix = taskId.getValue();
    postfix = postfix.substring(postfix.indexOf('.') + 1, postfix.length());
    postfix = postfix.substring(postfix.indexOf('.') + 1, postfix.length());
    driver.sendFrameworkMessage(
      ExecutorID.newBuilder().setValue("executor." + postfix).build(),
      slaveID,
      message.getBytes(Charset.defaultCharset()));
  }

  private boolean isTerminalState(TaskStatus taskStatus) {
    return taskStatus.getState().equals(TaskState.TASK_FAILED)
      || taskStatus.getState().equals(TaskState.TASK_FINISHED)
      || taskStatus.getState().equals(TaskState.TASK_KILLED)
      || taskStatus.getState().equals(TaskState.TASK_LOST)
      || taskStatus.getState().equals(TaskState.TASK_ERROR);
  }

  public String getMesosMasterUri() {
    return mesosMasterUri;
  }

  public String getCmd() {
    return cmd;
  }

  public List<Resource> getExecutorResources() {
    return executorResources;
  }
}
