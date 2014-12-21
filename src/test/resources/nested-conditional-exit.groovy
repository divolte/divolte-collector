/*
 * Copyright 2014 GoDataDriven B.V.
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

    section {
        when location().isPresent() apply {
            map 'happened' onto 'client'
            // Breaks out of this section
            exit()
            map 'should not happen' onto 'session'
        }
    }

    section {
        when location().isPresent() apply {
            map 'happened' onto 'pageview'
            when location().isAbsent() exit() // breaks out of the enclosing section
            map 'happened' onto 'event'
            when location().isPresent() exit() // breaks out of the enclosing section
            map 'should not happen' onto 'session'
        }
    }

    map 'happened' onto 'customCookie'
}
