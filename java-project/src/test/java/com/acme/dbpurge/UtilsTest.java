package com.acme.dbpurge;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;

import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UtilsTest  {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    public static Set<Integer> range(int start, int end) {
        return IntStream.range(start, end+1)
          .boxed()
          .collect(Collectors.toSet());
    }
    
    @Test
    public void testKeepNSmallestIdsFromSet() {

        int n  = 20;
        Set<Integer> setOneToN = UtilsTest.range(1,n);
        LOGGER.info("setOneToN" + setOneToN.toString());

        Set<Integer> setRes1 = Utils.keepNSmallestIdsFromSet(setOneToN, n+1 );
        assertTrue(CollectionUtils.isEqualCollection(setOneToN, setRes1));

        Set<Integer> setOneToFive = UtilsTest.range(1,5);
        Set<Integer> setRes2 = Utils.keepNSmallestIdsFromSet(setOneToN, 5 );
        assertTrue(CollectionUtils.isEqualCollection(setOneToFive, setRes2));
    }
}
