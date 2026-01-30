package com.aliyun.openservices.odps.console.rules;


import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class JdkVersionRule implements TestRule {

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        String version = System.getProperty("java.version");
        int majorVersion = getMajorVersion(version);

        // If the current JDK major version > 8, and the test method has the @IgnoreOnHighVersionJdk annotation
        if (majorVersion > 8 && description.getAnnotation(IgnoreOnHighVersionJdk.class) != null) {
          throw new org.junit.AssumptionViolatedException("Ignored on JDK higher than 1.8");
        }

        base.evaluate();
      }

      /**
       * Analysis of JDK main version number
       * @param version JDK version string (such as "1.8.0_302", "17.0.4", "21-ea")
       * @return Major version number (such as 8, 17, 21)
       */
      private int getMajorVersion(String version) {
        if (version.startsWith("1.")) {
          // JDK 1.x.y form, take the second part as the main version number
          String[] parts = version.split("\\.", 3);
          if (parts.length > 1) {
            return extractLeadingInteger(parts[1]);
          }
          return 0;
        } else {
          // JDK 9+, the main version number is the first part
          String[] parts = version.split("\\.", 2);
          return extractLeadingInteger(parts[0]);
        }
      }

      /**
       * Extract the continuous numerical part at the beginning of the string
       * @param s Input string (such as "21-ea", "17.0.4")
       * @return Extracted integer (such as 21, 17)
       */
      private int extractLeadingInteger(String s) {
        int result = 0;
        for (char c : s.toCharArray()) {
          if (Character.isDigit(c)) {
            result = result * 10 + (c - '0');
          } else {
            break;
          }
        }
        return result;
      }
    };
  }
}



