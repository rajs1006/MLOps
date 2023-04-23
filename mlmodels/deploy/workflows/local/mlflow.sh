printf "Enter the country name : "
read -r COUNTRY


DB_CONNECTION='postgresql://usr__sourabh_raj@aws-ec2-db-test-2b3681da:5432/algotrader?sslcert=/home/sourabh/Resonanz/Algotrader/env/sourabh/test/usr__sourabh_raj.crt&sslkey=/home/sourabh/Resonanz/Algotrader/env/sourabh/test/usr__sourabh_raj.key&sslrootcert=/home/sourabh/Resonanz/Algotrader/env/sourabh/test/ca.crt'
S3_PATH='s3://bayaga/${COUNTRY}/experiments/'

echo ""
echo "DB Connection string : ${DB_CONNECTION}"
echo "S3 Artifact store : ${S3_PATH}"
echo ""

printf "Enter port to run MLFlow, default(7009): "
read -r PORT
PORT=${PORT:=7009}

echo ""
mlflow server --backend-store-uri ${DB_CONNECTION} --default-artifact-root ${S3_PATH} --host 0.0.0.0 --port ${PORT}
