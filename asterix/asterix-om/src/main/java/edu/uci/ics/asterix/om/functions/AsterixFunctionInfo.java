/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.functions;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class AsterixFunctionInfo implements IFunctionInfo, Serializable {

    private FunctionIdentifier functionIdentifier;

    public AsterixFunctionInfo() {
        functionIdentifier = null;
    }

    public AsterixFunctionInfo(String namespace, AsterixFunction asterixFunction) {
        this.functionIdentifier = new FunctionIdentifier(namespace, asterixFunction.getFunctionName(),
                asterixFunction.getArity());
    }

    public AsterixFunctionInfo(FunctionIdentifier functionIdentifier) {
        this.functionIdentifier = functionIdentifier;
    }

    @Override
    public FunctionIdentifier getFunctionIdentifier() {
        return functionIdentifier;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AsterixFunctionInfo)) {
            return false;
        }
        AsterixFunctionInfo info = (AsterixFunctionInfo) o;
        return functionIdentifier.equals(info.getFunctionIdentifier());
    }

    @Override
    public String toString() {
        return this.functionIdentifier.getNamespace() + ":" + this.functionIdentifier.getName() + "@"
                + this.functionIdentifier.getArity();
    }

    /*
    private void writeObject(ObjectOutputStream os) {
        try {
            os.defaultWriteObject();
            os.writeObject(functionIdentifier);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void readObject(ObjectInputStream is) {
        try {
            is.defaultReadObject();
            functionIdentifier = (FunctionIdentifier) is.readObject();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }*/

}
