docker build -t controller .
kubectl apply -f resources.yaml
kubectl apply -f deployment.yaml