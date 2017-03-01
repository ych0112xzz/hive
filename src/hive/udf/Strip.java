package hive.udf;

/**
 * Created by ych0112xzz on 2017/3/1.
 */


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Strip extends UDF {
    public Text evaluate(String str) {
        return str == null ? null : new Text(StringUtils.strip(str));
    }

    public Text evaluate(String str, String chrStr) {
        return str == null ? null : new Text(StringUtils.strip(str, chrStr));
    }
}