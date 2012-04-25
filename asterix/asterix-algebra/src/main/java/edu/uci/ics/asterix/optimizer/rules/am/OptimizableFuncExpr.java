package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class OptimizableFuncExpr implements IOptimizableFuncExpr {
	protected final AbstractFunctionCallExpression funcExpr;
    protected final LogicalVariable[] logicalVars;
    protected final String[] fieldNames;
    protected final IAlgebricksConstantValue[] constantVals;
    
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable[] logicalVars, IAlgebricksConstantValue[] constantVals) {
    	this.funcExpr = funcExpr;
    	this.logicalVars = logicalVars;
    	this.constantVals = constantVals;
    	this.fieldNames = new String[logicalVars.length];
    }
    
    // Special, more convenient c'tor for simple binary functions.
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable logicalVar, IAlgebricksConstantValue constantVal) {
    	this.funcExpr = funcExpr;
    	this.logicalVars = new LogicalVariable[] { logicalVar };
    	this.constantVals = new IAlgebricksConstantValue[] { constantVal };
    	this.fieldNames = new String[logicalVars.length];
    }
    
	@Override
	public AbstractFunctionCallExpression getFuncExpr() {
		return funcExpr;
	}
	
	@Override
	public int getNumLogicalVars() {
		return logicalVars.length;
	}
	
	@Override
	public int getNumConstantVals() {
		return constantVals.length;
	}
	
	@Override	
	public LogicalVariable getLogicalVar(int index) {
		return logicalVars[index];
	}
	
	@Override
	public void setFieldName(int index, String fieldName) {
		fieldNames[index] = fieldName;
	}
	
	@Override
	public String getFieldName(int index) {
		return fieldNames[index];
	}
	
	@Override
	public IAlgebricksConstantValue getConstantVal(int index) {
		return constantVals[index];
	}
}
