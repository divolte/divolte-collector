mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'
    
    map cookie('custom_cookie') onto 'customCookie'
}
