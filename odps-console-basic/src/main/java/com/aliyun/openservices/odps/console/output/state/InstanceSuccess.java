package com.aliyun.openservices.odps.console.output.state;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.statemachine.State;
import com.google.gson.GsonBuilder;

/**
 * Instance 运行成功状态
 * 获取并输出 instance summary 信息
 *
 *
 * Created by zhenhong.gzh on 16/8/25.
 */
public class InstanceSuccess extends InstanceState {
  @Override
  public State run(InstanceStateContext context) throws OdpsException {
    // If using optimized key-path, this state should be skipped
    if (context.getExecutionContext().isLiteMode()) {
      return State.END;
    }

    try {
      Instance.TaskSummary taskSummary = getTaskSummaryV1(context.getOdps(), context.getInstance(),
                                     context.getTaskStatus().getName());

      context.setSummary(taskSummary);
      reportSummary(taskSummary, context.getExecutionContext().getOutputWriter());
    } catch (Exception ignore) {
    }

    return State.END;
  }

  private void reportSummary(Instance.TaskSummary taskSummary, DefaultOutputWriter writer) {
    // 输出summary信息
    try {

      if (taskSummary == null || StringUtils.isBlank(taskSummary.getSummaryText())) {
        return;
      }
      // print Summary
      String summary = taskSummary.getSummaryText().trim();

      writer.writeError("Summary:");
      writer.writeError(summary);

    } catch (Exception e) {
      writer.writeError("can not get summary. " + e.getMessage());
    }
  }

  static class MapReduce {
    String summary;
  }

  static class Item {
    public MapReduce mapReduce;
  }

  // XXX very dirty !!!
  // DO HACK HERE
  public static Instance.TaskSummary getTaskSummaryV1(Odps odps, Instance i, String taskName) throws Exception {
    RestClient client = odps.getRestClient();
    Map<String, String> params = new HashMap<String, String>();
    params.put("summary", null);
    params.put("taskname", taskName);
    String queryString = "/projects/" + i.getProject() + "/instances/" + i.getId();
    Response result = client.request(queryString, "GET", params, null, null);

    Instance.TaskSummary summary = null;
    Item item = new GsonBuilder().disableHtmlEscaping().create()
        .fromJson(new String(result.getBody()), Item.class);
    if (item.mapReduce != null && !StringUtils.isBlank(item.mapReduce.summary)) {
      summary = new Instance.TaskSummary();
      Field textFiled = summary.getClass().getDeclaredField("text");
      textFiled.setAccessible(true);
      textFiled.set(summary, item.mapReduce.summary);
    }
    return summary;
  }
}
