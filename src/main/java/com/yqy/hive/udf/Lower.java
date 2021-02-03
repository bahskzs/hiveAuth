package com.yqy.hive.udf;
/**
 * @author cat
 * @date 2020-10-30 23:52
 */

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Lower
 * @author cat
 * @date 2020-10-30 23:52
 */
public final class Lower extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        return new Text(s.toString().toLowerCase());
    }
}
