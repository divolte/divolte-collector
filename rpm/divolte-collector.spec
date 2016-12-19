#
# Spec file for producing a RPM for the Divolte Collector.
#
%define _topdir %(echo $PWD)/
%define _buildrootdir %{_tmppath}/BUILDROOT
%define snapshot %{nil}%{?snapshotVersion}

Name:           divolte-collector
Version:        0.5.0
Release:        %{?snapshotVersion:1}%{?!snapshotVersion:2}%{?dist}
Summary:        The Divolte click-stream collection agent.

License:        Apache License, Version 2.0
URL:            https://github.com/divolte/divolte-collector/

Group:          Applications

# (For some reason rpmbuild segfaults when trying to download this; download it manually.)
#Source0:        https://github.com/divolte/%{name}/archive/%{version}.tar.gz
Source0:        %{name}-%{version}%{snapshot}.tar.gz
Source1:        %{name}.conf
Source2:        %{name}.init
Source3:        %{name}.logback.xml
BuildArch:      noarch

# Although we depend on Java 8, we don't declare it to preserve the
# sanity of sysadmins everywhere.
Requires:         bash, initscripts, util-linux
Requires(pre):    shadow-utils
Requires(post):   chkconfig
Requires(preun):  chkconfig
Requires(preun):  initscripts
Requires(postun): initscripts

%description
Divolte provides a platform for collecting click-stream data. The data
can be published to HDFS and Kafka where real-time and historical analysis
can be performed.

This package contains the HTTP server component that collects the click-stream
events from browsers. It will be started automatically on system startup.

%prep
%setup -n %{name}-%{version}%{snapshot}
sed -e "s/scmVersion\.version/'%{version}%{snapshot}'/g" build.gradle > build.gradle.$$ && mv build.gradle.$$ build.gradle

%build
./gradlew --no-daemon --exclude-task test build

%install
rm -rf "%{buildroot}"

./gradlew --no-daemon --exclude-task test installDist

%{__mkdir_p} "%{buildroot}/etc/divolte"
%{__mkdir_p} "%{buildroot}/etc/rc.d/init.d"
%{__mkdir_p} "%{buildroot}/usr/bin"
%{__mkdir_p} "%{buildroot}/usr/share/divolte/bin"
%{__mkdir_p} "%{buildroot}/usr/share/divolte/lib"
%{__mkdir_p} "%{buildroot}/var/log/divolte"

%{__install} -m 0644 "%{S:1}" "%{buildroot}/etc/divolte/%{name}.conf"
%{__install} -m 0644 "%{S:3}" "%{buildroot}/etc/divolte/logback.xml"
%{__install} -m 0755 "%{S:2}" "%{buildroot}/etc/rc.d/init.d/%{name}"
%{__cp} -R build/install/%{name}/lib/* "%{buildroot}/usr/share/divolte/lib"
%{__cp} -R src/scripts/* "%{buildroot}/usr/share/divolte/bin"
%{__cp} -R src/dist/conf/* "%{buildroot}/etc/divolte"
%{__ln_s} ../share/divolte/bin/%{name} "%{buildroot}/usr/bin/%{name}"

%check
./gradlew --no-daemon test

%clean
./gradlew --no-daemon clean

%pre
getent group divolte >/dev/null || groupadd -r divolte
getent passwd divolte >/dev/null || \
    useradd -r -g divolte -d /opt/divolte -s /sbin/nologin \
    -c "Divolte collection service" divolte
exit 0

%post
/sbin/chkconfig --add divolte-collector

%preun
if [ $1 -eq 0 ] ; then
    /sbin/service divolte-collector stop >/dev/null 2>&1
    /sbin/chkconfig --del divolte-collector
fi

%postun
if [ "$1" -ge "1" ] ; then
    /sbin/service divolte-collector condrestart >/dev/null 2>&1 || :
fi

%files
%defattr(-,root,root,0755)

%dir /etc/divolte
%config(noreplace) /etc/divolte/%{name}.conf
%config(noreplace) /etc/divolte/logback.xml
/etc/rc.d/init.d/%{name}
/etc/divolte/*.example
/usr/bin/%{name}
%dir /usr/share/divolte
%dir /usr/share/divolte/bin
/usr/share/divolte/bin/*
%dir /usr/share/divolte/lib
/usr/share/divolte/lib/*.jar
%attr(-,divolte,divolte)  /var/log/divolte

%changelog
* Fri Sep 5 2014 Andrew Snare <andrewsnare@godatadriven.com> - 0.1-1
- Initial packaging.
