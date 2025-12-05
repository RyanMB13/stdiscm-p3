package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"video_project/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	numProducers = flag.Int("p", 1, "Number of producer threads")
	serverAddr   = flag.String("addr", "localhost:50051", "Consumer Address")
)

func uploadFile(client proto.VideoServiceClient, filePath string) {
	filename := filepath.Base(filePath)

	// 1. Calculate Hash (For Bonus 2)
	file, _ := os.Open(filePath)
	hasher := sha256.New()
	io.Copy(hasher, file)
	hashString := hex.EncodeToString(hasher.Sum(nil))
	file.Close()

	// Retry Loop (For Bonus 1: Backpressure)
	for {
		file, _ := os.Open(filePath)
		stream, err := client.UploadVideo(context.Background())
		if err != nil {
			log.Println("Consumer offline, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}

		buffer := make([]byte, 64*1024)
		// Send First Chunk with Hash
		n, _ := file.Read(buffer)
		stream.Send(&proto.VideoChunk{
			Filename: filename, Content: buffer[:n], ContentHash: hashString,
		})

		// Send Rest
		for {
			n, err := file.Read(buffer)
			if err == io.EOF { break }
			stream.Send(&proto.VideoChunk{Filename: filename, Content: buffer[:n]})
		}
		file.Close()

		res, err := stream.CloseAndRecv()
		if err != nil {
			log.Printf("[%s] Upload Error: %v", filename, err)
			break
		}

		// CHECK FOR QUEUE FULL
		if res.Message == "Queue Full" {
			log.Printf("[%s] Queue Full. Sleeping 5s...", filename)
			time.Sleep(5 * time.Second)
			continue // Retry loop
		}

		log.Printf("[%s] Status: %s", filename, res.Message)
		break
	}
}

func producerWorker(id int, wg *sync.WaitGroup, client proto.VideoServiceClient) {
	defer wg.Done()
	folder := fmt.Sprintf("folder%d", id)
	files, _ := os.ReadDir(folder)

	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".mp4" {
			uploadFile(client, filepath.Join(folder, f.Name()))
			time.Sleep(1 * time.Second)
		}
	}
}

func main() {
	flag.Parse()
	conn, _ := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := proto.NewVideoServiceClient(conn)

	var wg sync.WaitGroup
	for i := 1; i <= *numProducers; i++ {
		wg.Add(1)
		go producerWorker(i, &wg, client)
	}
	wg.Wait()
}