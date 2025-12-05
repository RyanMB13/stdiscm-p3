package main

import (
	"encoding/json"
	"flag"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"video_project/proto"

	"google.golang.org/grpc"
)

// -- Global State --
var (
	uploadedVideos []string
	uploadedHashes = make(map[string]string)
	videoMutex     sync.RWMutex
	jobQueue       chan *UploadJob
)

// Config Flags
var (
	numConsumers = flag.Int("c", 1, "Number of consumer threads")
	queueSize    = flag.Int("q", 5, "Max queue length")
	port         = flag.String("port", "50051", "gRPC port")
)

type UploadJob struct {
	stream     proto.VideoService_UploadVideoServer
	firstChunk *proto.VideoChunk
	done       chan error
}

type server struct {
	proto.UnimplementedVideoServiceServer
}

// 1. gRPC Handler
func (s *server) UploadVideo(stream proto.VideoService_UploadVideoServer) error {
	firstChunk, err := stream.Recv()
	if err != nil { return err }

	videoMutex.RLock()
	if existingName, exists := uploadedHashes[firstChunk.ContentHash]; exists {
		videoMutex.RUnlock()
		log.Printf("Duplicate Content Detected! (Matches %s). Rejecting.", existingName)
		return stream.SendAndClose(&proto.UploadStatus{Success: false, Message: "Duplicate Content"})
	}
	videoMutex.RUnlock()

	job := &UploadJob{stream: stream, firstChunk: firstChunk, done: make(chan error)}

	select {
	case jobQueue <- job:
		return <-job.done
	default:
		log.Println("Queue Full! Rejecting video.")
		return stream.SendAndClose(&proto.UploadStatus{Success: false, Message: "Queue Full"})
	}
}

// 2. Worker Logic
func consumerWorker(id int) {
	log.Printf("Consumer Thread %d started", id)
	for job := range jobQueue {
		job.done <- processVideo(job.stream, id, job.firstChunk)
	}
}

func processVideo(stream proto.VideoService_UploadVideoServer, id int, firstChunk *proto.VideoChunk) error {
	filename := firstChunk.Filename
	savePath := filepath.Join("uploads", filename)

	file, err := os.Create(savePath)
	if err != nil { return err }
	
	file.Write(firstChunk.Content)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF { break }
		if err != nil { file.Close(); return err }
		file.Write(chunk.Content)
	}
	file.Close()

	// Register Immediately
	videoMutex.Lock()
	uploadedVideos = append(uploadedVideos, filename)
	uploadedHashes[firstChunk.ContentHash] = filename
	videoMutex.Unlock()
	
	stream.SendAndClose(&proto.UploadStatus{Success: true, Message: "Uploaded"})

	// Background Compression
	log.Printf("[Worker %d] Compressing %s...", id, filename)
	compressedName := "compressed_" + filename
	cmd := exec.Command("ffmpeg", "-i", savePath, "-vcodec", "libx264", "-crf", "38", "-y", filepath.Join("uploads", compressedName))
	
	if err := cmd.Run(); err == nil {
		os.Rename(filepath.Join("uploads", compressedName), savePath)
		log.Printf("[Worker %d] Compression Done.", id)
	} else {
		log.Printf("[Worker %d] Compression failed.", id)
	}
	return nil
}

// 3. GUI Handler (Returns HTML)
func guiHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, _ := template.ParseFiles("consumer/templates/index.html")
	videoMutex.RLock()
	defer videoMutex.RUnlock()
	tmpl.Execute(w, uploadedVideos)
}

// 4. NEW API Handler (Returns JSON Data)
func videosHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	videoMutex.RLock()
	defer videoMutex.RUnlock()
	// Send the list of strings as JSON
	json.NewEncoder(w).Encode(uploadedVideos)
}

func main() {
	flag.Parse()
	os.Mkdir("uploads", 0755)
	jobQueue = make(chan *UploadJob, *queueSize)

	for i := 1; i <= *numConsumers; i++ {
		go consumerWorker(i)
	}

	lis, _ := net.Listen("tcp", ":"+*port)
	s := grpc.NewServer()
	proto.RegisterVideoServiceServer(s, &server{})
	go s.Serve(lis)

	// Register Handlers
	http.Handle("/uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))))
	http.HandleFunc("/api/videos", videosHandler)
	http.HandleFunc("/", guiHandler)
	
	log.Printf("Consumer running. GUI at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}