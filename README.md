# Event-Driven Microservices

A small event-driven microservices application built with **Python**, **FastAPI**, **Apache Kafka**, **Docker**, and **Kubernetes**.

The project demonstrates how independently deployable services can communicate through HTTP and asynchronous events. An order is submitted through the Order Service, published to Kafka, and consumed independently by the Inventory and Notification services.

## Architecture

```text
                         HTTP
  Client  ------------------------------+
                                         |
                                         v
                                  Order Service
                                         |
                                    OrderCreated
                                         |
                                         v
                                Kafka: order-events
                                   /          \
                                  /            \
                     Consumer group      Consumer group
                   inventory-service   notification-service
                            |                   |
                            v                   v
                    Inventory Service   Notification Service
```

### Services

| Service | Responsibility |
|---|---|
| **Order Service** | Exposes the order creation API and publishes order events to Kafka. |
| **Inventory Service** | Consumes order events and performs inventory-related processing. |
| **Notification Service** | Consumes order events and handles notification-related processing. |
| **Kafka** | Provides the event stream used for asynchronous communication between services. |

Each consumer service uses its own Kafka consumer group. This allows both services to receive the events independently. Multiple replicas of the same consumer service share a consumer group, so Kafka distributes partitions among those replicas.

## Technology stack

- **Python** — application language
- **FastAPI** — HTTP APIs
- **Apache Kafka** — event streaming and asynchronous messaging
- **Docker** — containerization
- **Kubernetes** — container orchestration
- **kind** — running a local Kubernetes cluster
- **kafka-python** — Kafka client library for Python

## Prerequisites

Install the following tools before running the project:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [kind](https://kind.sigs.k8s.io/)
- Python 3 and pip (for local development, if supported by the service setup)

The commands below are examples of the usual workflow. Resource names, image names, manifest paths, and environment variable names must match the configuration in this repository.

## Getting started

### 1. Start Kafka

Start the Kafka broker using the Compose configuration included in the repository:

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Check that the broker is running:

```bash
docker compose -f kafka/docker-compose.yml ps
docker compose -f kafka/docker-compose.yml logs -f
```

Create the topic used by the services if it is not created automatically:

```bash
docker exec <kafka-container> kafka-topics.sh \
  --create \
  --topic order-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

> **Kafka networking:** Kafka clients first connect to a bootstrap address and then use the broker address returned in Kafka metadata. The broker's advertised listener must therefore be reachable from the network where each client runs. `localhost` inside a container or Kubernetes Pod refers to that container or Pod, not to the host machine.

### 2. Build the service images

Build an image for each service using its Dockerfile. For example:

```bash
docker build -t order-service:local ./order-service
docker build -t inventory-service:local ./inventory-service
docker build -t notification-service:local ./notification-service
```

If you use kind, load the images into the cluster's node:

```bash
kind load docker-image order-service:local --name <cluster-name>
kind load docker-image inventory-service:local --name <cluster-name>
kind load docker-image notification-service:local --name <cluster-name>
```

Make sure the image names and tags in the Kubernetes Deployments match the images you build.

### 3. Deploy the applications to Kubernetes

Apply the Kubernetes manifests:

```bash
kubectl apply -f <kubernetes-manifests-directory>/
```

Check the deployment and Pod status:

```bash
kubectl get deployments
kubectl get pods -o wide
kubectl get services
```

Wait for the Deployments to become available:

```bash
kubectl rollout status deployment/order-service
kubectl rollout status deployment/inventory-service
kubectl rollout status deployment/notification-service
```

The Deployment names above should match the names in your manifests.

### 4. Send an order

Expose the Order Service locally with the access method configured by the Kubernetes Service. For example, using port-forwarding:

```bash
kubectl port-forward service/order-service 8000:8000
```

In another terminal, submit an order:

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": 1001, "quantity": 2}'
```

The request body must match the API schema implemented by the Order Service.

### 5. Verify event processing

Inspect the consumer logs:

```bash
kubectl logs deployment/inventory-service --tail=100
kubectl logs deployment/notification-service --tail=100
```

The same order event should be processed independently by both consumer groups.

## Useful commands

### Docker

| Command | Description |
|---|---|
| `docker ps` | List running containers. |
| `docker images` | List locally available images. |
| `docker logs -f <container>` | Follow a container's logs. |
| `docker exec -it <container> sh` | Open a shell in a running container, if the image includes one. |
| `docker compose up -d` | Start Compose services in the background. |
| `docker compose down` | Stop and remove Compose containers and networks. |

### Kubernetes

| Command | Description |
|---|---|
| `kubectl get nodes` | List cluster nodes. |
| `kubectl get pods -o wide` | List Pods and their placement/IP information. |
| `kubectl get deployments` | List Deployments and replica status. |
| `kubectl get services` | List Services and exposed ports. |
| `kubectl describe pod <pod>` | Inspect a Pod and its recent events. |
| `kubectl logs <pod>` | Print a Pod's logs. |
| `kubectl logs -f <pod>` | Follow a Pod's logs. |
| `kubectl exec -it <pod> -- sh` | Open a shell in a running Pod, if available. |
| `kubectl apply -f <file-or-directory>` | Create or update resources from manifests. |
| `kubectl delete -f <file-or-directory>` | Delete resources defined by manifests. |
| `kubectl port-forward service/<name> <local-port>:<service-port>` | Forward a local port to a Kubernetes Service. |
| `kubectl scale deployment/<name> --replicas=<count>` | Change the desired replica count. |
| `kubectl rollout status deployment/<name>` | Wait for a Deployment rollout to complete. |
| `kubectl rollout restart deployment/<name>` | Restart Pods managed by a Deployment. |

### kind

| Command | Description |
|---|---|
| `kind get clusters` | List local kind clusters. |
| `kind create cluster --name <name>` | Create a kind cluster. |
| `kind delete cluster --name <name>` | Delete a kind cluster. |
| `kind load docker-image <image> --name <cluster>` | Load a local Docker image into a kind cluster. |

## Kafka concepts used in this project

### Topics and partitions

A **topic** is a named stream of records. A topic is divided into **partitions**, which provide parallelism and determine the unit of ordering. Kafka guarantees ordering within a partition, not across all partitions.

### Consumer groups

Consumers with the same group ID cooperate to consume a topic. Each partition is assigned to at most one consumer within a group at a time. Separate consumer groups each receive the topic's records independently.

For example, if `order-events` has three partitions:

- Inventory replicas using group ID `inventory-service` share those partitions.
- Notification replicas using group ID `notification-service` form a separate group and independently consume the events.
- Adding more consumers than partitions to one group does not create more partition-level parallelism; some consumers may remain idle.

### Offsets

A consumer group tracks its progress using offsets. Offsets allow consumers to resume from their previously committed position after a restart. The behavior for uncommitted records depends on the consumer configuration and processing/commit strategy.

## Configuration

Application and infrastructure settings should be provided through environment variables or configuration files rather than hard-coded values.

Typical settings include:

- Kafka bootstrap server address
- Kafka topic name
- Kafka consumer group ID
- HTTP listening port
- Logging level

Refer to each service's configuration and deployment manifest for the exact variable names and required values. Do not commit credentials, tokens, or other secrets to the repository.

## Development and troubleshooting

A useful troubleshooting sequence is:

1. Check whether the relevant Docker container or Kubernetes Pod is running.
2. Check the application logs for startup errors.
3. Verify that the HTTP endpoint responds.
4. From the service container, verify Kafka hostname resolution and TCP connectivity.
5. Confirm that Kafka advertises an address reachable by the application.
6. Confirm that the producer successfully publishes the event.
7. Confirm that each intended consumer group receives and processes it.

A running Pod or successful HTTP health endpoint does not necessarily mean Kafka message processing is healthy. Health checks only verify the conditions they are explicitly configured to check.

## Scope and limitations

This repository is intended as a compact example and development environment, not as a production-ready deployment. A production system would need additional design and operational work, including (but not limited to):

- Kafka broker replication and availability
- Authentication, authorization, and encrypted communication
- Secret management
- Resource requests, limits, and autoscaling policies
- Readiness and liveness probes
- Graceful shutdown and message-processing lifecycle management
- Retry policies, dead-letter handling, and idempotent consumers
- Metrics, tracing, centralized logging, and alerting
- Persistent storage, backup, and recovery procedures
