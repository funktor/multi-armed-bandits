docker build -t dao-queue .
kubectl apply -f resources.yaml
kubectl apply -f deployment.yaml