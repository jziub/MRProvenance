package edu.indiana.d2i.lib.collector;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface ProvenanceCollector<INKEY extends Writable, INVALUE extends Writable, OUTKEY extends Writable, OUTVALUE extends Writable> {		
	public final double TIME_SCALE = 1000000000.0;
	
	public void collectInput(INKEY key, INVALUE value) throws IOException;
	public void collectOutput(OUTKEY key, OUTVALUE value) throws IOException;
	public void close() throws IOException;
}
