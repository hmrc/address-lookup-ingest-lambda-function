package com.jessecoyle;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class WildcardHelper {

    static Stream<String> credentialNamesMatchingWildcard(List<CredentialVersion> credentialVersions, String credential) {
        Pattern pattern = Pattern.compile('^' + credential.replace("*", ".*") + '$');
        return credentialVersions.stream()
                .map(CredentialVersion::getName)
                .distinct()
                .filter(name -> pattern.matcher(name).matches());
    }
}
