package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
/**
 * need this class to read data source line by line
 * using BufferedReader only doesn't really read line by line since it removes line feed ('\r', '\n')
 *
 * @author  glorifiedjx
 * 
 */
public class LineReader {
	private int i;
	private BufferedReader br;
	public LineReader(BufferedReader br) { this.br = br; }
	public String readLine() throws IOException {
		i = br.read();
		if (i < 0) return null;
		StringBuilder sb = new StringBuilder();
		sb.append((char)i);
		if (i != '\r' && i != '\n') {
			while (0 <= (i = br.read()) && i != '\r' && i != '\n') {
				sb.append((char)i);
			}
			if (i < 0) return sb.toString();
			sb.append((char)i);
		}
		if (i == '\r') {
			i = br.read();
			if (i != '\n') return sb.toString(); 
			sb.append((char)'\n');
		}
		return sb.toString();
	}
}
