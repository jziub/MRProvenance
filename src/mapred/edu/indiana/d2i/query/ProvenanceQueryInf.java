package edu.indiana.d2i.query;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public interface ProvenanceQueryInf {
	public Object query(Text key) throws IOException;
}
