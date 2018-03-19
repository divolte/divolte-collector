/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    def hdrArr = header('X-Divolte-Test-Array', ', ')
    map hdrArr.first() onto 'headerArrayFirst'
    map hdrArr.last() onto 'headerArrayLast'
    map hdrArr.get(-2) onto 'headerArraySecondLast'

    def hdr = header('X-Divolte-Test')
    map hdr onto 'headerList'
    map hdr.first() onto 'headerFirst'
    map hdr.get(1) onto 'headerGet'
    map hdr.get(-2) onto 'headerSecondLast'
    map hdr.last() onto 'headerLast'
    map hdr.commaSeparated() onto 'headers'
}
