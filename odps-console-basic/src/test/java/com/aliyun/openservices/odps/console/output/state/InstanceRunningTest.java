package com.aliyun.openservices.odps.console.output.state;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instances;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by zhenhong.gzh on 17/8/3.
 */
public class InstanceRunningTest {

  static int statusCount = 0;
  static int progressCount = 0;
  public static Instance.StageProgress progress;
  static long lastFailed = -1;
  final static long mockQueryTimeoutMillis = TimeUnit.SECONDS.toMillis(15);

  static {
    progress = Mockito.spy(new Instance.StageProgress());

    when(progress.getBackupWorkers()).thenReturn(0);
    when(progress.getFinishedPercentage()).thenReturn(2);
    doAnswer(new Answer() {
      private int count = 0;
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (count++ % 2  == 1) {
          return Instance.StageProgress.Status.TERMINATED;
        }

        return Instance.StageProgress.Status.RUNNING;
      }
    }).when(progress).getStatus();
    when(progress.getName()).thenReturn("test_stage");
    when(progress.getRunningWorkers()).thenReturn(1);
    when(progress.getTerminatedWorkers()).thenReturn(1);
    when(progress.getTotalWorkers()).thenReturn(2);
  }

  @Test
  public void testInstanceRunning_GetProgressFail() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());

    SQLTask task = new SQLTask();
    String taskname = "sqltest";
    task.setQuery("select count(*) from src;");
    task.setName(taskname);

    Instance instance = Mockito.spy(instances.create(task));
    statusCount = 0;
    progressCount = 0;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        statusCount ++;

        if (statusCount > 15) {
          return Instance.Status.TERMINATED;
        }

        return Instance.Status.RUNNING;
      }
    }).when(instance).getStatus(true);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        progressCount ++;

        if (progressCount == 1) {
          return null;
        }
        if (progressCount == 2) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          return res;
        }

        if (progressCount == 3 || progressCount == 4) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(progress);
          return res;
        }

        if (progressCount == 5) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(progress);
          res.add(progress);
          return res;
        }

        if (progressCount > 6) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(progress);
          return res;
        }

        throw new OdpsException("get progress failed.");
      }
    }).when(instance).getTaskProgress(taskname);

    InstanceStateContext cont = Mockito.spy(new InstanceStateContext(odps, instance, context));
    InstanceRunning running = Mockito.spy(new InstanceRunning());
    running.run(cont);

    Assert.assertEquals(1, cont.getTaskProgress().size());
  }

  @Test(expected = ReloadException.class)
  public void testInstanceRunning_GetStatusTimeout() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());

    SQLTask task = new SQLTask();
    String taskname = "sqltest";
    task.setQuery("select count(*) from src;");
    task.setName(taskname);

    Instance instance = Mockito.spy(instances.create(task));
    Field field = InstanceRunning.class.getDeclaredField("STATUS_QUERY_TIMEOUT");
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(InstanceRunning.class, mockQueryTimeoutMillis);
    statusCount = 0;
    progressCount = 0;

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        statusCount ++;

        if (statusCount >= 2 && statusCount <=4) {
          throw new ReloadException("get status failed");
        }

        if (statusCount == 7) {
          lastFailed = System.currentTimeMillis();
          throw new ReloadException("get status failed");
        }

        if (statusCount == 8) {
          Thread.sleep(mockQueryTimeoutMillis);
          throw new ReloadException("get status failed");
        }

        return Instance.Status.RUNNING;
      }
    }).when(instance).getStatus(true);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        progressCount ++;

        if (progressCount >= 3) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(progress);
          return res;
        }

        return null;
      }
    }).when(instance).getTaskProgress(taskname);

    InstanceStateContext cont = Mockito.spy(new InstanceStateContext(odps, instance, context));
    InstanceRunning running = new InstanceRunning();
    try {
      running.run(cont);

    } catch (ReloadException e) {
      Assert.assertEquals(8, statusCount);
      Field field1 = running.getClass().getDeclaredField("firstFailed");
      field1.setAccessible(true);
      Assert.assertTrue(((Long) field1.get(running) - lastFailed) < 5);

      throw e;
    }

    Assert.fail();
  }
}
