package com.aliyun.datalake.metastore.common.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DataLakeUtil {

    public static String wrapperPatternString(String pattern) {
        return pattern == null ? ".*" : pattern;
    }

    public static String wrapperNullString(String s) {
        return s == null ? "" : s;
    }

    public static <T> List<T> wrapperNullList(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    public static boolean isEnableFileOperationGray(double grayRate) {
        if (grayRate >= 1.0d || ThreadLocalRandom.current().nextDouble() < grayRate) {
            return true;
        } else {
            return false;
        }
    }

    public static <T extends Throwable> T throwException(T ex, Exception e) {
        ex.initCause(e);
        return ex;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen = length(cs);
        if (strLen == 0) {
            return true;
        } else {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    public static int length(CharSequence cs) {
        return cs == null ? 0 : cs.length();
    }
}
