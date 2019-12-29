package se.fnord;


public class IdentityDecoderFunction<FROM> implements DecoderFunction<FROM, FROM> {
	@Override
	public FROM decode(FROM from) {
		return from;
	}
}
