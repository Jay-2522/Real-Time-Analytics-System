FROM confluentinc/cp-kafka:7.6.14@sha256:b20da2c030172c556f1a5569ef70110e1b2937dcc03356a90da93ffa4467791f
# Apply supported UBI package errata without replacing vendor Kafka/Java libraries.
USER 0
RUN microdnf update -y && microdnf clean all
USER 1000:1000
