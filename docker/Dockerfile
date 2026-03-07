
FROM apache/hadoop:3


WORKDIR /opt/kmeans

COPY ../src/kmeans.jar ./kmeans.jar
COPY ../data/iris_features.csv ./iris_features.csv
COPY entrypoint.sh ./entrypoint.sh


RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]