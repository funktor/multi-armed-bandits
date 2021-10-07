docker build -t dao-db .
kubectl apply -f resources.yaml
kubectl apply -f deployment.yaml