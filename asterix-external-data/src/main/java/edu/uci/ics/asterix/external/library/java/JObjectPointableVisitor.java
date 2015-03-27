package edu.uci.ics.asterix.external.library.java;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.java.JObjectAccessors.JListAccessor;
import edu.uci.ics.asterix.external.library.java.JObjectAccessors.JRecordAccessor;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class JObjectPointableVisitor implements
        IVisitablePointableVisitor<IJObject, Triple<IObjectPool<IJObject, IAType>, IAType, ATypeTag>> {

    private final Map<ATypeTag, IJObjectAccessor> flatJObjectAccessors = new HashMap<ATypeTag, IJObjectAccessor>();
    private final Map<IVisitablePointable, IJRecordAccessor> raccessorToJObject = new HashMap<IVisitablePointable, IJRecordAccessor>();
    private final Map<IVisitablePointable, IJListAccessor> laccessorToPrinter = new HashMap<IVisitablePointable, IJListAccessor>();
    
    public final static JObjectPointableVisitor INSTANCE = new JObjectPointableVisitor();
    
    /* 
     * Make this object a singleton to avoid
     * excessive instantiations
     */  
    private JObjectPointableVisitor() {
        
    }

    @Override
    public IJObject visit(AListPointable accessor, Triple<IObjectPool<IJObject, IAType>, IAType, ATypeTag> arg)
            throws AsterixException {
        IJObject result = null;
        IJListAccessor jListAccessor = laccessorToPrinter.get(accessor);
        if (jListAccessor == null) {
            jListAccessor = new JListAccessor();
            laccessorToPrinter.put(accessor, jListAccessor);
        }
        try {
            result = jListAccessor.access(accessor, arg.first, arg.second, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(ARecordPointable accessor, Triple<IObjectPool<IJObject, IAType>, IAType, ATypeTag> arg)
            throws AsterixException {
        IJObject result = null;
        IJRecordAccessor jRecordAccessor = raccessorToJObject.get(accessor);
        if (jRecordAccessor == null) {
            jRecordAccessor = new JRecordAccessor();
            raccessorToJObject.put(accessor, jRecordAccessor);
        }
        try {
            result = jRecordAccessor.access(accessor, arg.first, (ARecordType) arg.second, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(AFlatValuePointable accessor, Triple<IObjectPool<IJObject, IAType>, IAType, ATypeTag> arg)
            throws AsterixException {
        ATypeTag typeTag = arg.third;
        IJObject result = null;
        IJObjectAccessor jObjectAccessor = flatJObjectAccessors.get(typeTag);
        if (jObjectAccessor == null) {
            jObjectAccessor = JObjectAccessors.createFlatJObjectAccessor(typeTag);
            flatJObjectAccessors.put(typeTag, jObjectAccessor);
        }

        try {
            result = jObjectAccessor.access(accessor, arg.first);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        return result;
    }
    
    public void reset() {
        flatJObjectAccessors.clear();
        raccessorToJObject.clear();
        laccessorToPrinter.clear();
    }

}
