package br.com.anteros.collections.utils;

import java.util.concurrent.Semaphore;

public class AdjustableSemaphore extends Semaphore {

    int numberOfPermits = 0;

    public AdjustableSemaphore() {
        super(0, true);
    }

    public synchronized void setMaxPermits(int desiredPermits) {
        if (desiredPermits > numberOfPermits)
            release(desiredPermits - numberOfPermits);
        else if (desiredPermits < numberOfPermits)
            reducePermits(numberOfPermits - desiredPermits);
        numberOfPermits = desiredPermits;
    }
};