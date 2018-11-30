package cn.edu360.spark;

/**
 * Created by WeiYang on 2018/11/26.
 *
 * @Author: WeiYang
 * @Package cn.edu360.spark
 * @Project: SparkTest
 * @Title:
 * @Description: Please fill description of the file here
 * @Date: 2018/11/26 18:35
 */
public class Plus {

    public static int[] plusOne(int[] digits) {

        return plusOne(digits, digits.length - 1);
    }


    public static int[] plusOne(int[] digits, int index) {

        Integer res = digits[index] + 1;

        if (res < 10) {
            digits[index] = res;
        } else {
            digits[index] = 0;

            if (index == 0) {
                int[] ret = new int[digits.length + 1];
                ret[0] = 1;
                digits = ret;
            } else {
                digits = plusOne(digits, index - 1);
            }
        }
        return digits;
    }


    public static void main(String[] args) {

        int[] list = {1, 3, 9, 3};

        int[] out = plusOne(list);

        for (int i = 0; i < out.length; i++) {
            System.out.println(out[i]);
        }

    }


}
