FROM postgres:16

# Add banner entrypoint wrapper
COPY infrastructure/docker/banner-entrypoint.sh /usr/local/bin/banner-entrypoint.sh
RUN chmod +x /usr/local/bin/banner-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/banner-entrypoint.sh"]
CMD ["postgres"]
