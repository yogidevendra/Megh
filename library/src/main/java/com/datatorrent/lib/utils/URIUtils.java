package com.datatorrent.lib.utils;

import java.net.URI;

import com.google.common.base.Splitter;

public class URIUtils
{
  /**
   * Converts Scheme part of the URI to lower case. Multiple URI can be comma
   * separated. If no scheme is there, no change is made.
   * 
   * @param
   * @return String with scheme part as lower case
   */
  public static String convertSchemeToLowerCase(String uri)
  {
    if (uri == null)
      return null;
    StringBuilder inputMod = new StringBuilder();
    for (String f : Splitter.on(",").omitEmptyStrings().split(uri)) {
      String scheme = URI.create(f).getScheme();
      if (scheme != null) {
        inputMod.append(f.replaceFirst(scheme, scheme.toLowerCase()));
      } else {
        inputMod.append(f);
      }
      inputMod.append(",");
    }
    inputMod.setLength(inputMod.length() - 1);
    return inputMod.toString();
  }
}
