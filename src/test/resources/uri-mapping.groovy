mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    
    def locationUri = parse location() to uri
    map locationUri.scheme() onto 'uriScheme'
    map locationUri.path() onto 'uriPath'
    map locationUri.host() onto 'uriHost'
    map locationUri.port() onto 'uriPort'
    
    map locationUri.decodedQueryString() onto 'uriQueryString'
    map locationUri.query().value('q') onto 'uriQueryStringValue'
    map locationUri.query().valueList('p') onto 'uriQueryStringValues'
    map locationUri.query() onto 'uriQuery'

    
    def refererUri = parse referer() to uri
    map refererUri.decodedFragment() onto 'uriFragment'
}