package com.yongqing.common.bigdata.tool;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ListUtil {
    public static List getPagingList(List list,Integer start,Integer length){
        start = start<0?0:start;
        //默认为10
        length = length<=0?10:length;
        Integer size = list.size();
        if(start>size){
            start = size;
        }
        Integer toIndex = (start+length-1)>=size?size:(start+length-1);
        if(toIndex<=0){
            toIndex = size;
        }
        return list.subList(start,toIndex);
    }
    public static <T> List<List<T>> averageAssign(List<T> source, int n) {
        if(source.size()<n){
            n=source.size();
        }
        List<List<T>> result = new ArrayList<List<T>>();
        int remainder = source.size() % n;  //(先计算出余数)
        int number = source.size() / n;  //然后是商
        int offset = 0;//偏移量
        for (int i = 0; i < n; i++) {
            List<T> value = null;
            if (remainder > 0) {
                value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
                remainder--;
                offset++;
            } else {
                value = source.subList(i * number + offset, (i + 1) * number + offset);
            }
            result.add(value);
        }
        return result;
    }
}
