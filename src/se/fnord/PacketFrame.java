package se.fnord;

public interface PacketFrame {
	PayloadFrame parentFrame();

	PcapRecord rootFrame();
}