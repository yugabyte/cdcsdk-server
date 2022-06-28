FROM registry.access.redhat.com/ubi8/openjdk-11

LABEL maintainer="Yugabyte"

#
# Set the version, home directory, and MD5 hash.
#
ENV SERVER_HOME=/cdcsdk-server

#
# Create a directory for Debezium Server
#
USER root
RUN microdnf -y install gzip && \
    microdnf clean all && \
    mkdir $SERVER_HOME && \
    chmod 755 $SERVER_HOME

#
# Change ownership and switch user
#
RUN chown -R jboss $SERVER_HOME && \
    chgrp -R jboss $SERVER_HOME
USER jboss

RUN mkdir $SERVER_HOME/conf && \
    mkdir $SERVER_HOME/data

#
# Download and install Debezium Server
#
COPY target/cdcsdk-server-dist-*.tar.gz /tmp/cdcsdk-server.tar.gz

#
# Verify the contents and then install ...
#
RUN tar xzf /tmp/cdcsdk-server.tar.gz -C $SERVER_HOME --strip-components 1

#
# Allow random UID to use Debezium Server
#
RUN chmod -R g+w,o+w $SERVER_HOME

# Set the working directory to the Debezium Server home directory
WORKDIR $SERVER_HOME

#
# Expose the ports and set up volumes for the data, transaction log, and configuration
#
EXPOSE 8080
VOLUME ["/cdcsdk-server/conf","/cdcsdk-server/data"]

CMD ["/cdcsdk-server/run.sh"]
