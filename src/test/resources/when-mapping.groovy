mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'
    
    when { location() equalTo 'http://www.example.com/' } apply {
        map 'locationmatch' onto 'eventType'
        // nested when
        when { referer() equalTo 'http://www.example.com/somepage.html' } apply {
            map 'referermatch' onto 'client'
        }
    }
    
    when { referer() equalTo 'not the referer' } apply {
        map 'is set' onto 'queryparam'
    }
}