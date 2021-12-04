package co.baihai.dag;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: halo-pipeline
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 11:39
 */
public class MaxRetryStillFailedExecption extends Exception {
    public MaxRetryStillFailedExecption(String name) {
        super(name);
    }
}
