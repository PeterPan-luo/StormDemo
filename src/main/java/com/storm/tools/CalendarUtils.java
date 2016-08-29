package com.storm.tools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Date Aug 6, 2015
 *
 * @Author dengjie
 *
 * @Note CalendarUtils class
 */
public class CalendarUtils {

	public static String today() {
		SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMdd");
		return dfs.format(new Date());
	}

}
