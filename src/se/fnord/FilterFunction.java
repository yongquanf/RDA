package se.fnord;


public interface FilterFunction<T, U extends T> {
	public boolean test(T from);
}
