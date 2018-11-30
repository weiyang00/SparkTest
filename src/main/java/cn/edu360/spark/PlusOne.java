package cn.edu360.spark;

import org.w3c.dom.stylesheets.LinkStyle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by WeiYang on 2018/11/26.
 *
 * @Author: WeiYang
 * @Package cn.edu360.spark
 * @Project: SparkTest
 * @Title:
 * @Description: Please fill description of the file here
 * @Date: 2018/11/26 17:32
 */
public class PlusOne {

    public static int[] plusOne(int[] digits) {
        //从数组最后一个元素开始
        for (int i = digits.length - 1; i >= 0; i--) {
            //如果最后一个元素 小于9 进行+1操作 返回即可 例:123、456、677
            if (digits[i] < 9) {
                digits[i]++;
                return digits;
            } else {
                digits[i] = 0;
            }
        }
        //创建新数组
        int[] res = new int[digits.length + 1];
        //对第一位数字 赋值为1 例:99、999、9999
        res[0]++;
        return res;
    }

    public static void main(String[] args) {


        int[] list = {9, 9, 9};

        int[] out = plusOne(list);

        for (int i = 0; i < out.length; i++) {
            System.out.println(out[i]);
        }


    }


}
