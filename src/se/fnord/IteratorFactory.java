package se.fnord;

import java.util.Iterator;

public interface IteratorFactory<FROM, TO> {
	Iterator<TO> map(Iterator<FROM> from);
}
