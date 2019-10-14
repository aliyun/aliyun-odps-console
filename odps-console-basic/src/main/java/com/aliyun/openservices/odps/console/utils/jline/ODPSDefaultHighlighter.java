package com.aliyun.openservices.odps.console.utils.jline;

import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import java.util.ArrayList;
import java.util.List;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * This class highlights all the command keywords & odps sql reserved words
 * @author jon (wangzhong.zw@alibaba-inc.com)
 */
public class ODPSDefaultHighlighter implements Highlighter {
  private static final AttributedStyle STYLE = AttributedStyle.DEFAULT
      .bold()
      .foreground(AttributedStyle.MAGENTA);
  private static final AttributedStyle STYLE_OFF = AttributedStyle.DEFAULT
      .boldOff()
      .foregroundOff();

  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    AttributedStringBuilder sb = new AttributedStringBuilder();

    int currentIndex = 0;
    for (Pair<Integer, Integer> range : getIndexRangesToHighlight(buffer)) {
      if (range.first > currentIndex) {
        sb.append(buffer.substring(currentIndex, range.first));
      } else if (range.first < currentIndex) {
        continue;
      }
      sb.style(STYLE);
      sb.append(buffer.substring(range.first, range.second));
      sb.style(STYLE_OFF);
      currentIndex = range.second;
    }
    sb.append(buffer.substring(currentIndex));
    return sb.toAttributedString();
  }

  private List<Pair<Integer, Integer>> getIndexRangesToHighlight(String buffer) {
    buffer = buffer.toUpperCase();
    List<String> highlightedWords = new ArrayList<>(CommandParserUtils.getAllCommandKeyWords());

    List<Pair<Integer, Integer>> indexRanges = new ArrayList<>();
    for (String reservedWord : highlightedWords) {
      int start;
      int end = 0;
      while((start = buffer.indexOf(reservedWord.toUpperCase(), end)) != -1) {
        end = start + reservedWord.length();
        // make sure the substring to highlight is a word
        if (start == 0 || Character.isWhitespace(buffer.charAt(start - 1))) {
          if (end == buffer.length() || Character.isWhitespace(buffer.charAt(end)) || buffer.charAt(end) == ';') {
            indexRanges.add(new Pair<>(start, end));
          }
        }
      }
    }
    return mergeIndexRanges(indexRanges);
  }

  private List<Pair<Integer, Integer>> mergeIndexRanges(List<Pair<Integer, Integer>> indexRanges) {
    List<Pair<Integer, Integer>> mergedIndexRanges = new ArrayList<>();

    indexRanges.sort((o1, o2) -> {
      if (o1.first < o2.first) {
        return -1;
      } else if (o1.first > o2.first) {
        return 1;
      } else {
        // Try to match the longer keyword
        if (o1.second > o2.second) {
          return -1;
        } else if (o1.second < o2.second) {
          return 1;
        } else {
          return 0;
        }
      }
    });

    for (int i = 0; i < indexRanges.size(); i++) {
      int start = indexRanges.get(i).first;
      int end = indexRanges.get(i).second;
      for (int j = i + 1; j < indexRanges.size(); j++) {
        int curStart = indexRanges.get(j).first;
        int curEnd = indexRanges.get(j).second;
        if (curStart <= end) {
          end = Math.max(curEnd, end);
          // Skip current range
          i = j;
        } else {
          break;
        }
      }
      mergedIndexRanges.add(new Pair<>(start, end));
    }
    return mergedIndexRanges;
  }
}
