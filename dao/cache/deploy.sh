docker build -t dao-cache .
kubectl apply -f resources.yaml
kubectl apply -f deployment.yaml