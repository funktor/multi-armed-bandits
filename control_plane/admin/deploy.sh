docker build -t admin .
kubectl apply -f resources.yaml
kubectl apply -f consumer.yaml
kubectl apply -f migrate.yaml