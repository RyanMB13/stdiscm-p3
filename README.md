# P3 --- Networked Producer and Consumer (Go Implementation)

## System Requirements

1.  **Producer Machine:** macOS Apple Silicon / M-Series assumed) with
    Go installed.
2.  **Consumer Machine:** Ubuntu Virtual Machine (ARM64) with FFmpeg
    installed.
3.  **Network:** Both machines must be on the same network (**Bridged
    Adapter recommended**).

------------------------------------------------------------------------

# Part 1: Preparation (One-Time Setup)

## A. On Ubuntu VM (Server)

### 1. Install FFmpeg

``` sh
sudo apt update
sudo apt install ffmpeg
```

### 2. Find your IP address

``` sh
ip addr
```

Example: `192.168.64.8` --- **you will need this later**.

------------------------------------------------------------------------

## B. On macOS (Client)

### 1. Create test folders for the producer threads

``` sh
mkdir folder1
for i in {2..5}; do cp -r folder1 folder$i; done
```

### 2. Add test videos

Place at least one `.mp4` video inside `folder1`.

------------------------------------------------------------------------

# Part 2: Deployment

## 1. Build the Consumer for Linux (on macOS)

``` sh
GOOS=linux GOARCH=arm64 go build -o consumer-linux consumer/main.go
```

## 2. Transfer Files to the VM (from macOS)

Replace `<IP>` with your VM's IP:

``` sh
ssh ryan@<IP> "mkdir -p ~/video_project"
scp -r consumer-linux consumer/templates ryan@<IP>:~/video_project/
```

------------------------------------------------------------------------

# Part 3: Running the System

## Step 1: Start the Consumer (Server)

### 1. SSH into VM

``` sh
ssh ryan@<IP>
```

### 2. Navigate to the project directory

``` sh
cd ~/video_project
```

### 3. Make binary executable

``` sh
chmod +x consumer-linux
```

### 4. Run the Consumer

``` sh
./consumer-linux -c 2 -q 5
```

------------------------------------------------------------------------

## Step 2: Start the Producer (Client)

``` sh
go run producer/main.go -p 2 -addr "<IP>:50051"
```

------------------------------------------------------------------------

## Step 3: View the GUI

Open your browser and go to:

    http://<IP>:8080

------------------------------------------------------------------------

# Testing Bonus Features

## 1. Queue Full (Backpressure Simulation)

``` sh
./consumer-linux -c 1 -q 1
go run producer/main.go -p 5 -addr "<IP>:50051"
```

Expected behavior:

    Queue Full. Sleeping 5s...

## 2. Duplicate Detection

Expected behavior:

    Upload Failed: Duplicate Content

## 3. Video Compression

``` sh
ls -lh ~/video_project/uploads
```
