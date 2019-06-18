package com.kineticdata.bridgehub.adapter.aveksa;

import com.kineticdata.bridgehub.adapter.QualificationParser;

public class AveksaQualificationParser extends QualificationParser {
    public String encodeParameter(String name, String value) {
        return value;
    }
}
