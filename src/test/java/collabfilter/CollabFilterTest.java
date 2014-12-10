package collabfilter;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CollabFilterTest {
	private CollabFilterCassandraDriver cfc;

	@Before
	public void setUp() {
		this.cfc = new CollabFilterCassandraDriver();
		this.cfc.populateTables();
	}

	@After
	public void tearDown() {
		this.cfc.truncateTables();
	}

	@Test
	public void integratedTest_java7() throws Exception {
		integratedTest(7);
	}

	@Test
	public void integratedTest_java8() throws Exception {
		integratedTest(8);
	}

	private void integratedTest(int version) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		double rmse = this.cfc.trainAndValidate(version);

		assertTrue("Excess Root mean square error, was " + rmse, rmse < 0.5);
		assertTrue("Root mean square error should be non-negative, was" + rmse, rmse >= 0.0);

	}

}
