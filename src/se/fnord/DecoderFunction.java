package se.fnord;


public interface DecoderFunction<FROM, TO> {
	public TO decode(FROM from);
}
