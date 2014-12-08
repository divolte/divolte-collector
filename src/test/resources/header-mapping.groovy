mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'
    
    def hdr = header('X-Divolte-Test')
    map hdr onto 'headerList'
    map hdr.first() onto 'header'
    map hdr.commaSeparated() onto 'headers'
}
