package com.aliyun.openservices.odps.console.output.state;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

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

      if (taskSummary == null || "".equals(taskSummary.toString().trim())) {
        return;
      }
      // print Summary
      String summary = taskSummary.getSummaryText();

      writer.writeError("Summary:");
      writer.writeError(summary);

    } catch (Exception e) {
      writer.writeError("can not get summary. " + e.getMessage());
    }
  }

  // XXX very dirty !!!
  // DO HACK HERE
  private Instance.TaskSummary getTaskSummaryV1(Odps odps, Instance i, String taskName) throws Exception {
    RestClient client = odps.getRestClient();
    Map<String, String> params = new HashMap<String, String>();
    params.put("summary", null);
    params.put("taskname", taskName);
    String queryString = "/projects/" + i.getProject() + "/instances/" + i.getId();
    Response result = client.request(queryString, "GET", params, null, null);

    Instance.TaskSummary summary = null;
    Map map = new GsonBuilder().disableHtmlEscaping().create()
            .fromJson(new String(result.getBody()), Map.class);
    if (map != null && map.get("mapReduce") != null) {
      Map mapReduce = (Map) map.get("mapReduce");
      String jsonSummary = (String) mapReduce.get("jsonSummary");
      summary = new Instance.TaskSummary();
      if (jsonSummary == null) {
        jsonSummary = "{}";
      }
      Field textFiled = summary.getClass().getDeclaredField("text");
      textFiled.setAccessible(true);
      textFiled.set(summary, mapReduce.get("summary"));
      Field jsonField = summary.getClass().getDeclaredField("jsonSummary");
      jsonField.setAccessible(true);
      jsonField.set(summary, jsonSummary);
    }
    return summary;
  }
}
