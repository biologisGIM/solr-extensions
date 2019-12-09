package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;

public class Util {

    public static StreamEqualitor convertToEqualitor(StreamComparator comp) {
        if (!(comp instanceof MultipleFieldComparator)) {
            FieldComparator fComp = (FieldComparator)comp;
            return new FieldEqualitor(fComp.getLeftFieldName(), fComp.getRightFieldName());
        } else {
            MultipleFieldComparator mComp = (MultipleFieldComparator)comp;
            StreamEqualitor[] eqs = new StreamEqualitor[mComp.getComps().length];

            for(int idx = 0; idx < mComp.getComps().length; ++idx) {
                eqs[idx] = convertToEqualitor(mComp.getComps()[idx]);
            }

            return new MultipleFieldEqualitor(eqs);
        }
    }
}
