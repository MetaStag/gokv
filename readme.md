![](https://raw.githubusercontent.com/egonelbre/gophers/master/.thumb/vector/fairy-tale/witch-learning.png)


### gokv

a distributed key-value api written in golang

database used: [badger](https://github.com/hypermodeinc/badger)

---

#### Key-Value API

This project implements a simple key-value api with support for distributed nodes
- A simple HTTP API is exposed with 3 endpoints for managing key-value pairs
- The backend runs on multiple nodes, each with their own copy of the database for fault tolerance
- Docker containers are used to simulate nodes
- Nodes connect to each other via HTTP requests
- A simple commit algorithm is implemented where each change is propagated to every other node (not practical for real use)
- It uses a Write-Ahead Log (WAL) for durability

#### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone http://github.com/metastag/gokv.git
   cd gokv
   ```

2. **Install dependencies:**
   ```bash
   # make sure you have go and docker installed
   go mod tidy
   ```

3. **Start docker containers**
   ```bash
   # if you want to modify the cluster, update docker-compose.yml
   docker-compose up
   ```

#### Usage

- **Set a key-value pair:**
  ```
  GET /set?key=<key>&value=<value>
  ```

- **Get a value by key:**
  ```
  GET /get?key=<key>
  ```

- **Delete a key-value pair:**
  ```
  GET /delete?key=<key>
  ```
