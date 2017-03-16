package com.codeondemand.javapeppers.aleppo.action;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;

import java.sql.CallableStatement;
import java.sql.Connection;

public abstract class ExecuteDBProcedure extends RecordProcessor {

    @Override
    public RecordCapsule processRecord(RecordCapsule input) {

        return input;
    }

    @Override
    public abstract boolean doInitialization();

    protected abstract boolean setParameters();

    protected abstract boolean executeProcedure();

    protected String decryptPWD(String input) {
        return input;
    }

    protected Connection con = null;
    protected String proc = null;
    protected CallableStatement stmt = null;


}
