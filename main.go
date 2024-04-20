package main

import (
	"log"
	"time"

	"github.com/joho/godotenv"
)

func init() {
	loadEnv()
}

func main() {
	go Produce()
	go Consume()

	time.Sleep(10 * time.Minute)
}

func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}
