ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version

LABEL maintainer="edriggers@splicemachine.com"

ARG BENCHMARK_HOME=/opt/htap

RUN mkdir -p ${BENCHMARK_HOME} && yum -y install gettext
ADD template-config.xml ${BENCHMARK_HOME}/
ADD target/*.jar ${BENCHMARK_HOME}/target/
ADD log4j.properties ${BENCHMARK_HOME}/

ADD run-benchmark.sh ${BENCHMARK_HOME}/run-benchmark.sh

WORKDIR ${BENCHMARK_HOME} 

ENTRYPOINT ["/bin/bash", "-c", "./run-benchmark.sh \"$@\"", "--"]