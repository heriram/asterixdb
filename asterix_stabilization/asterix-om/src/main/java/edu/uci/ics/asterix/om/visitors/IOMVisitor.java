package edu.uci.ics.asterix.om.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.ABinary;
import edu.uci.ics.asterix.om.base.ABitArray;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ACircle;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.asterix.om.base.ALine;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.APoint3D;
import edu.uci.ics.asterix.om.base.APolygon;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.types.IAType;

public interface IOMVisitor {
    public void visitABoolean(ABoolean obj) throws AsterixException;

    public void visitADouble(ADouble obj) throws AsterixException;

    public void visitAFloat(AFloat obj) throws AsterixException;

    public void visitAInt8(AInt8 obj) throws AsterixException;

    public void visitAInt16(AInt16 obj) throws AsterixException;

    public void visitAInt32(AInt32 obj) throws AsterixException;

    public void visitAInt64(AInt64 obj) throws AsterixException;

    public void visitAString(AString obj) throws AsterixException;

    public void visitADuration(ADuration obj) throws AsterixException;

    public void visitADate(ADate obj) throws AsterixException;

    public void visitATime(ATime obj) throws AsterixException;

    public void visitADateTime(ADateTime obj) throws AsterixException;

    public void visitABitArray(ABitArray obj) throws AsterixException;

    public void visitABinary(ABinary obj) throws AsterixException;

    public void visitAOrderedList(AOrderedList obj) throws AsterixException;

    public void visitAUnorderedList(AUnorderedList obj) throws AsterixException;

    public void visitARecord(ARecord obj) throws AsterixException;

    public void visitANull(ANull obj) throws AsterixException;

    public void visitAPoint(APoint obj) throws AsterixException;

    public void visitAPoint3D(APoint3D obj) throws AsterixException;

    public void visitAType(IAType obj) throws AsterixException;

    public void visitALine(ALine obj) throws AsterixException;

    public void visitAPolygon(APolygon obj) throws AsterixException;

    public void visitACircle(ACircle obj) throws AsterixException;

    public void visitARectangle(ARectangle obj) throws AsterixException;
}
