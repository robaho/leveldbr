FROM scratch
ADD keydbrd /
EXPOSE 8501
CMD ["/leveldbr"]
