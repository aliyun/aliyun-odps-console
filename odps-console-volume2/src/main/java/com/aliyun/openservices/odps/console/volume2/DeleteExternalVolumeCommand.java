package com.aliyun.openservices.odps.console.volume2;

import java.io.PrintStream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class DeleteExternalVolumeCommand extends ExternalVolumeCommand {

  public static void printDeleteExternalVolumeUsage(PrintStream out) {
    out.println("");
    out.println("Usage: vfs <-rmv> <volume_path>; remove invalid or illegal external volume.");
  }

  public DeleteExternalVolumeCommand(String commandText,
                                     ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    String[] paras = getCommandText().split("\\s+");
    if (paras.length != 3) {
      printDeleteExternalVolumeUsage(System.err);
      throw new ODPSConsoleException("Invalid delete external volume command.");
    }

    Odps odps = getCurrentOdps();
    String path = paras[2];

    rmFile(odps, path);
    getWriter().writeError("OK");
  }

  private void rmFile(Odps odps, String path) throws ODPSConsoleException, OdpsException {
    int start = path.startsWith("/") ? 1 : 0;
    String[] sp = path.substring(start).split("/");
    if (sp.length > 1) {
      throw new ODPSConsoleException("Invalid parameters - can not remove file or folder.");
    }

    String vName = sp[0];
    if (!StringUtils.isNullOrEmpty(vName)) {
      Project project = odps.projects().get();
      odps.volumes().delete(project.getName(), vName);
    } else {
      throw new ODPSConsoleException("Invalid parameters - unrecognized option [" + path + "].");
    }
  }
}
