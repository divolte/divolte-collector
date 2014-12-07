mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    
    def locationUri = parse location() to uri
    def hashUri = parse locationUri.rawFragment() to uri
    
    map hashUri.path() onto 'uriPath'
    map hashUri.rawQueryString() onto 'uriQueryString'
    map hashUri.query().value('q') onto 'uriQueryStringValue'
}