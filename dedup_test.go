package dedup

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

const testDirPath = "./testdir/"

func init() {
	createTestFiles(250)
}
func createTestFiles(num int) {
	for i := 0; i < num; i++ {
		// Create New Filename using UUID-V4
		filename := testDirPath + uuid.NewV4().String()
		ext := ".txt"
		f, err := os.Create(filename + ext)
		if err != nil {
			log.Fatal(err)
		}
		fbuf := bufio.NewWriter(f)
		randBuf := make([]byte, 100000)
		if _, err = rand.Read(randBuf); err != nil {
			log.Fatal(err)
		}
		_, err = fbuf.Write(randBuf)
		if err != nil {
			log.Fatal(err)
		}
		f.Close()
		// Duplicate even files
		if i%2 == 0 {
			i++
			orig, err := os.Open(filename + ext)
			defer orig.Close()
			if err != nil {
				log.Fatal(err)
			}
			dup, err := os.Create(filename + "_dup_" + ext)
			defer dup.Close()
			if err != nil {
				log.Fatal(err)
			}
			_, err = io.Copy(dup, orig)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
func TestGetDirFiles(t *testing.T) {
	result, err := getDirFiles(testDirPath)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	for i := 0; i < 10; i++ {
		fmt.Printf("Filename %v: %s\n", i, result[i])
	}
}

func TestFindDuplicates(t *testing.T) {
	results, err := FindDuplicates(testDirPath)
	assert.Nil(t, err)
	for i := 0; i < len(results); i++ {
		fmt.Printf("File %v: %s Hash: %s Dupl: %v\n", i, results[i].Pathname, results[i].Blake2bHEX, results[i].DupOfPosition)
	}
}

func TestDeduplicateToNew(t *testing.T) {
	err := DeduplicateToNew(testDirPath, testDirPath+"Deduped")
	assert.Nil(t, err)
}

func TestDeduplicateByDeletion(t *testing.T) {
	err := DeduplicateByDeletion(testDirPath)
	assert.Nil(t, err)
}
