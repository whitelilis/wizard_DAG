package co.baihai.dag;

/**
 * Copyright BaiHai.ai (c) all right reserved.
 * Project: halo-pipeline
 * Author   : lz@baihai.ai
 * Created: 2021-12-04 10:24
 */
public enum Status {
    SUCCESS(true, true),
    FAILED(true, false),
    TIMEOUT(true, false),
    READY(false, false),
    EXCEPTION(false, false),
    RUNNING(false, false);

    boolean isFinished;
    boolean isSuccessful;
    boolean isFailed;

    Status(boolean isFinished, boolean isSuccessful){
        this.isFinished = isFinished;
        this.isSuccessful = isSuccessful;
        this.isFailed = !isSuccessful;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }

    public boolean isFailed() {
        return isFailed;
    }
}
