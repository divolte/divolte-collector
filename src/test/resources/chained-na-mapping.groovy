mapping {
    // fields that do not have defaults
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'
    
    // referer is null in the expected request
    def refUri = parse referer() to uri
    def refPath = refUri.path()    
    def refPathMatcher = match 'some regex with a (group)' against refPath
    
    // we use the queryparam field, because it has a non-null default
    // which we expect to be preserved
    map refPathMatcher.group(1) onto 'queryparam'
}