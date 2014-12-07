mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    
    def locationUri = parse location() to uri
    map locationUri.rawPath() onto 'uriPath'
    map locationUri.rawQueryString() onto 'uriQueryString'
    map locationUri.rawFragment() onto 'uriFragment'
}