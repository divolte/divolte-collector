mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'


    def regex = '^http://[^/]+/path/with/([0-9]+)/(?<page>[^\\.]+)\\.html'
    def locMatcher = match regex against location()
    
    map locMatcher.matches() onto 'pathBoolean'
    map locMatcher.group(1) onto 'client'
    map locMatcher.group("page") onto 'pageview'
}