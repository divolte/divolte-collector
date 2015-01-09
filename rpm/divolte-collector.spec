#
# Spec file for producing a RPM for the Divolte Collector.
#
%define _topdir %(echo $PWD)/
%define _buildrootdir %{_tmppath}/BUILDROOT
%define snapshot %{nil}%{?snapshotVersion}

Name:           divolte-collector
Version:        0.2
Release:        1%{?dist}
Summary:        The Divolte click-stream collection agent.

License:        Apache License, Version 2.0
URL:            https://github.com/divolte/divolte-collector/

Group:          Applications

# (For some reason rpmbuild segfaults when trying to download this; download it manually.)
#Source0:        https://github.com/divolte/%{name}/archive/%{version}.tar.gz
Source0:        %{name}-%{version}%{snapshot}.tar.gz
Source1:        %{name}.default
Source2:        %{name}.conf
Source3:        %{name}.init
Source4:        %{name}.logback.xml
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

%build
./gradlew -x test build

%install
rm -rf "%{buildroot}"

./gradlew -x test installApp

%{__mkdir_p} "%{buildroot}/etc/default"
%{__mkdir_p} "%{buildroot}/etc/divolte"
%{__mkdir_p} "%{buildroot}/etc/init.d"
%{__mkdir_p} "%{buildroot}/usr/bin"
%{__mkdir_p} "%{buildroot}/usr/share/divolte/bin"
%{__mkdir_p} "%{buildroot}/usr/share/divolte/lib"
%{__mkdir_p} "%{buildroot}/var/log/divolte"

%{__install} -m 0644 "%{S:1}" "%{buildroot}/etc/default/%{name}"
%{__install} -m 0644 "%{S:2}" "%{buildroot}/etc/divolte/%{name}.conf"
%{__install} -m 0644 "%{S:4}" "%{buildroot}/etc/divolte/logback.xml"
%{__install} -m 0755 "%{S:3}" "%{buildroot}/etc/init.d/%{name}"
%{__cp} -R build/install/%{name}/lib/* "%{buildroot}/usr/share/divolte/lib"
%{__ln_s} ../share/divolte/bin/%{name} "%{buildroot}/usr/bin/%{name}"
awk '{print}
$0~/^DEFAULT_JVM_OPTS=/ {
  print "# Customized during RPM build."
  print "DEFAULT_JVM_OPTS='\\\"'$DEFAULT_JVM_OPTS -Dconfig.file=/etc/divolte/divolte-collector.conf'\\\"'"
  print "[ -r /etc/default/divolte-collector ] && . /etc/default/divolte-collector"
}
$0~/^CLASSPATH=/ {
  print "# Customized during RPM build. Adding HADOOP_CONF_DIR to classpath if set."
  print "[ -n '\\\"'$HADOOP_CONF_DIR'\\\"' ] && CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH"
}
' build/install/%{name}/bin/%{name} > "%{buildroot}/usr/share/divolte/bin/%{name}"
%{__chmod} 0755 "%{buildroot}/usr/share/divolte/bin/%{name}"

%check
./gradlew test

%clean
./gradlew clean

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

%config /etc/default/%{name}
%dir /etc/divolte
%config /etc/divolte/%{name}.conf
%config /etc/divolte/logback.xml
%config /etc/init.d/%{name}
/usr/bin/%{name}
%dir /usr/share/divolte
%dir /usr/share/divolte/bin
/usr/share/divolte/bin/%{name}
%dir /usr/share/divolte/lib
/usr/share/divolte/lib/*.jar
%attr(-,divolte,divolte)  /var/log/divolte

%changelog
* Fri Sep 5 2014 Andrew Snare <andrewsnare@godatadriven.com> - 0.1-1
- Initial packaging.
