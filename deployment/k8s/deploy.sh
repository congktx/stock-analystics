#!/bin/bash
# Kubernetes Deployment Script for Stock Analytics
# This script deploys Kafka, Flink and related services to Kubernetes

set -e

echo "================================================"
echo "Stock Analytics - Kubernetes Deployment"
echo "================================================"
echo ""

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed or not in PATH"
    echo "Please install kubectl: https://kubernetes.io/docs/tasks/tools/"
    exit 1
fi

# Check if kubectl can connect to cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster"
    echo "Please ensure your cluster is running and kubectl is configured"
    exit 1
fi

echo "✓ kubectl is installed and connected to cluster"
echo ""

# Navigate to k8s directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
K8S_DIR="$SCRIPT_DIR/k8s"

if [ ! -d "$K8S_DIR" ]; then
    echo "Error: k8s directory not found at $K8S_DIR"
    exit 1
fi

cd "$K8S_DIR"

echo "Deploying resources from: $K8S_DIR"
echo ""

# Create namespace
echo "1. Creating namespace..."
kubectl apply -f namespace.yaml
echo ""

# Create ConfigMap
echo "2. Creating ConfigMap..."
kubectl apply -f configmap.yaml
echo ""

# Deploy Zookeeper
echo "3. Deploying Zookeeper..."
kubectl apply -f zookeeper.yaml
echo "   Waiting for Zookeeper to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/zookeeper -n stock-analytics
echo ""

# Deploy Kafka
echo "4. Deploying Kafka..."
kubectl apply -f kafka.yaml
echo "   Waiting for Kafka to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/kafka -n stock-analytics
echo ""

# Deploy Kafka UI
echo "5. Deploying Kafka UI..."
kubectl apply -f kafka-ui.yaml
kubectl wait --for=condition=available --timeout=60s deployment/kafka-ui -n stock-analytics
echo ""

# Deploy Flink
echo "6. Deploying Flink (JobManager and TaskManager)..."
kubectl apply -f flink.yaml
echo "   Waiting for Flink JobManager to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/flink-jobmanager -n stock-analytics
echo "   Waiting for Flink TaskManager to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/flink-taskmanager -n stock-analytics
echo ""

echo "================================================"
echo "Deployment Complete!"
echo "================================================"
echo ""
echo "Services accessible at:"
echo "  - Kafka Broker: localhost:30092"
echo "  - Kafka UI: http://localhost:30080"
echo "  - Flink Dashboard: http://localhost:30081"
echo ""
echo "To view all resources:"
echo "  kubectl get all -n stock-analytics"
echo ""
echo "To view logs:"
echo "  kubectl logs -f deployment/kafka -n stock-analytics"
echo "  kubectl logs -f deployment/flink-jobmanager -n stock-analytics"
echo ""
echo "To delete all resources:"
echo "  kubectl delete namespace stock-analytics"
echo ""
