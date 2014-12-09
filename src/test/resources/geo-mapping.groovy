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
    
    def geo = ip2geo()
    map geo.cityId() onto 'geoCityId'
    map geo.cityName() onto 'geoCityName'
    map geo.continentCode() onto 'geoContinentCode'
    map geo.continentId() onto 'geoContinentId'
    map geo.continentName() onto 'geoContinentName'
    map geo.countryCode() onto 'geoCountryCode'
    map geo.countryId() onto 'geoCountryId'
    map geo.countryName() onto 'geoCountryName'
    map geo.latitude() onto 'geoLatitude'
    map geo.longitude() onto 'geoLongitude'
    map geo.metroCode() onto 'geoMetroCode'
    map geo.timeZone() onto 'geoTimeZone'
    map geo.mostSpecificSubdivisionCode() onto 'geoMostSpecificSubdivisionCode'
    map geo.mostSpecificSubdivisionId() onto 'geoMostSpecificSubdivisionId'
    map geo.mostSpecificSubdivisionName() onto 'geoMostSpecificSubdivisionName'
    map geo.postalCode() onto 'geoPostalCode'
    map geo.registeredCountryCode() onto 'geoRegisteredCountryCode'
    map geo.registeredCountryId() onto 'geoRegisteredCountryId'
    map geo.registeredCountryName() onto 'geoRegisteredCountryName'
    map geo.representedCountryCode() onto 'geoRepresentedCountryCode'
    map geo.representedCountryId() onto 'geoRepresentedCountryId'
    map geo.representedCountryName() onto 'geoRepresentedCountryName'
    map geo.subdivisionCodes() onto 'geoSubdivisionCodes'
    map geo.subdivisionIds() onto 'geoSubdivisionIds'
    map geo.subdivisionNames() onto 'geoSubdivisionNames'
    map geo.autonomousSystemNumber() onto 'geoAutonomousSystemNumber'
    map geo.autonomousSystemOrganization() onto 'geoAutonomousSystemOrganization'
    map geo.domain() onto 'geoDomain'
    map geo.isp() onto 'geoIsp'
    map geo.organisation() onto 'geoOrganisation'
    map geo.anonymousProxy() onto 'geoAnonymousProxy'
    map geo.satelliteProvider() onto 'geoSatelliteProvider'
}
