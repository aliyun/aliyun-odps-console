package com.aliyun.openservices.odps.console.utils.jline;

import com.aliyun.odps.utils.StringUtils;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;

/**
 * This class is used to enable multiline-editing
 */
public class ODPSDefaultParser extends DefaultParser {
  @Override
  public ParsedLine parse(final String line, final int cursor, ParseContext context) {
    if (!StringUtils.isNullOrEmpty(line) && !line.endsWith(";")
        && context != ParseContext.COMPLETE) {
      throw new EOFError(
          -1, -1, "Missing ending semicolon", "missing semicolon");
    }
    return super.parse(line, cursor, context);
  }
}
