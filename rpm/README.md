Divolte RPM Packaging
=====================

Packaging RPMs for Divolte is done in the old-fashioned way with `rpmbuild`.
Before an RPM can be built, a source release for Divolte must be available. If
necessary, this can be created using git:

    git archive --format=tar.gz -9 --prefix=divolte-collector-$VERSION/ release-$VERSION --output divolte-collector-$VERSION.tar.gz

Once a source release is available, it must be placed into this directory.
An RPM can then be built:

    rpmbuild --target noarch-redhat-linux -ba divolte-collector.spec

This will produce both a source and binary RPM.

Frequently Asked Questions
==========================

Q. Divolte already builds with Gradle. Why not generate the RPM directly?
-------------------------------------------------------------------------

We'd love to do this. However…

The googles reveal 2 Gradle plugins, one of which is the anointed successor
of the other. At the time of writing, neither work with Gradle 2.x. Even if
they did work, apparently they (through necessity!) make use of private
Gradle APIs that frequently change between Gradle releases.

While we'd love to resolve these problems ourselves but it's not our area of
expertise and a working RPM is something we need right now.

Q. I get an error claiming 'intended for a different operating system' during install. How do I fix this?
---------------------------------------------------------------------------------------------------------

You need to specify `--target noarch-redhat-linux` when invoking `rpmbuild`.

RPMs are built for a specific architecture and operating system. Although the
architecture can be specified in the spec-file as `noarch` to match any
architecture, the operating system is implicitly the same as the platform being
used to build the RPM. Specifying the target as above informs `rpmbuild` that
the RPM is intended to target the `linux` operating system.
