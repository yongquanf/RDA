package se.fnord;


public interface PcapRecord extends PayloadFrame {
	long timestamp();

	int index();
}