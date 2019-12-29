package se.fnord;

public interface AddressFactory<T extends Address> {
	public T create();
}
