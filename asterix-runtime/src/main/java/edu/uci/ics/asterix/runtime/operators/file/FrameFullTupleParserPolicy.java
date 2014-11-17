package edu.uci.ics.asterix.runtime.operators.file;

import java.nio.ByteBuffer;
import java.util.Map;

import edu.uci.ics.asterix.common.parse.ITupleParserPolicy;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameFullTupleParserPolicy implements ITupleParserPolicy {

	private FrameTupleAppender appender;
	private ByteBuffer frame;
	private IFrameWriter writer;

	public void configure(Map<String, String> configuration) {
		// no-op
	}

	public void initialize(IHyracksTaskContext ctx, IFrameWriter writer)
			throws HyracksDataException {
		this.appender = new FrameTupleAppender(ctx.getFrameSize());
		this.frame = ctx.allocateFrame();
		this.writer = writer;
		appender.reset(frame, true);
	}

	public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
		boolean success = appender.append(tb.getFieldEndOffsets(),
				tb.getByteArray(), 0, tb.getSize());
		if (!success) {
			FrameUtils.flushFrame(frame, writer);
			appender.reset(frame, true);
			success = appender.append(tb.getFieldEndOffsets(),
					tb.getByteArray(), 0, tb.getSize());
			if (!success) {
				throw new IllegalStateException();
			}
		}
	}

	public void close() throws HyracksDataException {
		if (appender.getTupleCount() > 0) {
			FrameUtils.flushFrame(frame, writer);
		}

	}

	@Override
	public TupleParserPolicyType getType() {
		return TupleParserPolicyType.FRAME_FULL;
	}
}