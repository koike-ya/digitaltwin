rm -r deploy-oura2bq-gcf
mkdir deploy-oura2bq-gcf
cp gcp-credentials.json oura2bq_gcf/*.py oura2bq_gcf/requirements.txt deploy-oura2bq-gcf/
gcloud functions deploy oura2bq --trigger-http --runtime python38 --entry-point main --source deploy-oura2bq-gcf/