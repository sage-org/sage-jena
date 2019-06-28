package org.gdd.sage;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LiteralsRegexTest {
    @Test
    public void testTypeRegex () {
        Pattern typePattern = Pattern.compile("\"(.*)\"(\\^\\^)(.+)");
        String[] tests = {"\"help\"^^http:toto.com", "\"help\"^^<http:toto.com>"};
        String[] expected = {"http:toto.com", "<http:toto.com>"};
        int i = 0;
        for (String test : tests) {
            Matcher matcher = typePattern.matcher(test);
            Assert.assertEquals(true, matcher.matches());
            Assert.assertEquals(3, matcher.groupCount());
            Assert.assertEquals(expected[i], matcher.group(3));
            i++;
        }
    }
    @Test
    public void testLangRegex () {
        Pattern typePattern = Pattern.compile("\"(.*)\"(@)(.+)");
        String[] tests = {"\"help\"@en"};
        String[] expected = {"en"};
        int i = 0;
        for (String test : tests) {
            Matcher matcher = typePattern.matcher(test);
            Assert.assertEquals(true, matcher.matches());
            Assert.assertEquals(3, matcher.groupCount());
            Assert.assertEquals(expected[i], matcher.group(3));
            i++;
        }
    }
}
