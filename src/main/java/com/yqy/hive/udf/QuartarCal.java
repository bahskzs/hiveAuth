package com.yqy.hive.udf;/**
 * @author cat
 * @date 2020-11-26 11:35
 */

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * QuartarCal
 * @author cat
 * @date 2020-11-26 11:35
 */
public class QuartarCal extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }

        int res = (int) Math.ceil(Integer.parseInt(s.toString().substring(4,5))/3);

        return new Text(String.valueOf(res));
    }
}
