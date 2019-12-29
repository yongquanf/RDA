package se.fnord;

public interface DecoderFunctionSelector<FROM, TO> {
	DecoderFunction<FROM, ? extends TO> select(FROM value);
}
