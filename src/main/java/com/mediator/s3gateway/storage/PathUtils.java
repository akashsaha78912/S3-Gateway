package com.mediator.s3gateway.storage;

public class PathUtils {

    /**
     * Replaces characters illegal in Windows file systems with safe percent-encodings.
     */
    public static String sanitizeKeyForWindows(String key) {
        if (key == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : key.toCharArray()) {
            switch (c) {
                case ':' -> sb.append("%3A");
                case '*' -> sb.append("%2A");
                case '?' -> sb.append("%3F");
                case '"' -> sb.append("%22");
                case '<' -> sb.append("%3C");
                case '>' -> sb.append("%3E");
                case '|' -> sb.append("%7C");
                default  -> sb.append(c);
            }
        }
        return sb.toString();
    }
}