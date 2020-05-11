ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

LABEL maintainer="edriggers@splicemachine.com"

ARG BENCHMARK_HOME=/opt/htap

RUN mkdir -p ${BENCHMARK_HOME} && yum -y install gettext
ADD ./target/oltpbench-1.0.tar.gz ${BENCHMARK_HOME}
RUN mv ${BENCHMARK_HOME}/oltpbench-1.0/* ${BENCHMARK_HOME} && rmdir ${BENCHMARK_HOME}/oltpbench-1.0 

COPY template-config.xml log4j.properties ${BENCHMARK_HOME}/
COPY run-benchmark.sh ${BENCHMARK_HOME}/run-benchmark.sh

WORKDIR ${BENCHMARK_HOME} 

ENTRYPOINT ["/bin/bash", "-c", "./run-benchmark.sh \"$@\"", "--"]