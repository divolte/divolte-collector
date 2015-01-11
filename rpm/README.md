Divolte RPM Packaging
=====================

Packaging RPMs for Divolte is done in the old-fashioned way with `rpmbuild`. On a Mac, you can install rpm and rpmbuild using Homebrew (brew install rpm).

The top-level gradle build script has a task for setting up the SOURCES directory and invoking rpmbuild in the correct way. This is the easiest way to build an RPM:

  ./gradlew buildRpm

This will create a rpm in rpm/RPMS/noarch/. Note that the RPM will always have a release version number and drop the -SNAPSHOT if present.
