package com.baiduwaimai.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by Super腾 on 2016/12/15.
 */
public class TomasFunction extends UDF {
    public Text evaluate(Text a,Text b){
        return new Text(a.toString()+"*******"+b.toString());
    }

}
